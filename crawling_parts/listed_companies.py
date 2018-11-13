# -*- coding: utf-8 -*
import csv
import itertools
import shutil
import logging
from pathlib import Path
from multiprocessing.pool import Pool
from multiprocessing import Manager

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import get_control_file, put_control_file
from throttle import Throttle

ALPHABET_LIST = list(map(chr, range(65, 91)))
NUMBERS_LIST = list(range(0, 10))

COMPANIES_LISTING_SEARCHER_LETTERS = ALPHABET_LIST + NUMBERS_LIST

COMPANIES_LISTING_URL = "http://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/" \
                        "CiaAb/FormBuscaCiaAbOrdAlf.aspx?LetraInicial={}"

# The list of all the already processed letters (searching companies)
COMPANY_LETTERS_CTL = "ctl/listed_companies_letters.ctl"

# The list of all the already processed companies by letter
COMPANIES_CTL = "ctl/listed_companies_companies.ctl"

manager = Manager()
lock = manager.Lock()

_logger = logging.getLogger("bovespa")


def update_listed_companies_checkpoint(letter, companies=None):
    with lock:
        current_letters = get_control_file(COMPANY_LETTERS_CTL, [])
        if letter not in set(current_letters):
            current_letters.append(letter)
            put_control_file(COMPANY_LETTERS_CTL, current_letters)

        if companies:
            current_companies = get_control_file(COMPANIES_CTL, {})
            current_companies[letter] = companies
            put_control_file(COMPANIES_CTL, current_companies)


def has_letter(letter):
    with lock:
        current_letters = get_control_file(COMPANY_LETTERS_CTL, [])
        return letter in current_letters


@Throttle(minutes=1, rate=50, max_tokens=50)
def update_listed_companies(letter, phantomjs_path):
    driver = None
    try:
        companies = []
        driver = webdriver.PhantomJS(
            executable_path=phantomjs_path)

        url = COMPANIES_LISTING_URL.format(letter)

        # Let's navigate to the url and wait until the page is completely
        # loaded. We control that the page is loaded looking for the
        #  presence of the table with id = "dlCiasCdCVM"
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'dlCiasCdCVM')))
        except:
            WebDriverWait(driver, 10).until(
                EC.text_to_be_present_in_element(
                    (By.ID, 'lblMsg'),
                    "Nenhuma companhia foi encontrada com o critério de"
                    " busca especificado."))

            update_listed_companies_checkpoint(letter)
            return companies

        bs = BeautifulSoup(driver.page_source, "html.parser")

        companies_table = bs.find("table", attrs={"id": "dlCiasCdCVM"})
        companies_rows = companies_table.findChildren(["tr"])

        # The first row is the header
        for row in companies_rows[1:]:
            cells = row.findChildren('td')
            companies.append({
                "cnpj": cells[0].find("a").getText(),
                "name": cells[1].find("a").getText(),
                "type": cells[2].find("a").getText(),
                "ccvm": cells[3].find("a").getText(),
                "situation": cells[4].find("a").getText(),

            })

        update_listed_companies_checkpoint(letter, companies)
        return companies
    finally:
        _logger.debug("Finishing to crawl listed companies for letter {}".
              format(letter))
        if driver:
            _logger.debug("Closing the phantomjs driver for letter {}".
                          format(letter))
            driver.quit()


def crawl_listed_companies(phantomjs_path, workers_num=10, force=False):

    companies_already_crawled = []
    companies = []
    pool = Pool(processes=workers_num)
    try:
        if force:
            # We move the checkpoint files to start the crawling process
            if Path(COMPANY_LETTERS_CTL).exists():
                shutil.move(
                    COMPANY_LETTERS_CTL, "{}.bak".format(COMPANY_LETTERS_CTL))
            if Path(COMPANIES_CTL).exists():
                shutil.move(
                    COMPANIES_CTL, "{}.bak".format(COMPANIES_CTL))

        # We will launch a process per letter to crawl all the company data for
        # each letter
        func_params = []
        for letter in COMPANIES_LISTING_SEARCHER_LETTERS:
            # We only crawl the letter if it was not already
            # processed (checkpoint)
            if not has_letter(letter):
                # Preparing arguments for call the crawling function for the
                # current letter
                func_params.append([letter, phantomjs_path])
            else:
                # Loading the company data from the checkpoint
                current_companies = get_control_file(COMPANIES_CTL, [])
                if letter in current_companies.keys():
                    companies_already_crawled += current_companies[letter]

        # Start the pool of processes to crawl the information about companies
        # for each letter
        call_results = pool.starmap(update_listed_companies, func_params)

        # Merge all the responses into one only list
        companies += list(
            itertools.chain.from_iterable(call_results))

        # Add the companies already processed (checkpoint)
        companies += companies_already_crawled

        with open("data/companies.csv", "w") as f:
            headers = ["ccvm", "name", "cnpj", "type", "situation"]
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(companies)
    except TimeoutError:
        _logger.exception("Timeout error")
        raise
    finally:
        pool.close()
        pool.join()
        pool.terminate()
