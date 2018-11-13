# -*- coding: utf-8 -*
import csv
import re
import itertools
import shutil
import logging
from pathlib import Path
from multiprocessing.pool import Pool
from multiprocessing import Manager

from urllib.parse import urlencode
from dateutil.parser import parse as date_parse

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from utils import get_control_file, put_control_file
from throttle import Throttle

RE_DOWNLOAD_FILE = r"javascript:fVisualizaArquivo_ENET\('([\d]+)','DOWNLOAD'\)"

RE_FISCAL_DATE = r'Data Encerramento.*[\s].*(\d{2}/\d{2}/\d{4})'
RE_DELIVERY_DATE = r'Data Entrega.*[\s].*(\d{2}/\d{2}/\d{4}) \d{2}:\d{2}'
RE_VERSION = r'Versão.*[\s].*(\d+.\d+)'
RE_DELIVERY_TYPE = r'Tipo Apresentação.*[\s].*<td.*>([\w\s]*)</td>'
RE_COMPANY_NAME = r'Razão Social.*:(.*)<br/>'
RE_CNPJ = r'CNPJ.*:(.*)\s'
RE_TOTAL_FILES = r'(\d*) documento\(s\) encontrado\(s\)'
RE_LAST_FILE_IN_PAGE = r'Exibindo (\d*) a (\d*)'

COMPANY_DOCUMENTS_URL = "http://siteempresas.bovespa.com.br/consbov/" \
                        "ExibeTodosDocumentosCVM.asp?{}"

FILES_BY_COMPANY_CTL = "ctl/files_per_company.ctl"

manager = Manager()
lock = manager.Lock()

_logger = logging.getLogger("bovespa")


def update_companies_files_checkpoint(ccvm_code, doc_type, files=None):
    _logger.debug("Calling cache files from [{ccvm} - {doc_type}]: "
                  "{files}".
                  format(ccvm=ccvm_code,
                         doc_type=doc_type,
                         files=files))
    with lock:
        if files is not None:
            _logger.debug("Adding files from [{ccvm} - {doc_type}] to cache: "
                          "{files}".
                          format(ccvm=ccvm_code,
                                 doc_type=doc_type,
                                 files=files))
            current_companies = get_control_file(FILES_BY_COMPANY_CTL, {})
            key = "{0}_{1}".format(ccvm_code, doc_type)
            current_companies[key] = files
            put_control_file(FILES_BY_COMPANY_CTL, current_companies)
        else:
            _logger.debug("Files NOT ADDED from [{ccvm} - {doc_type}]"
                          " to cache: {files}".
                          format(ccvm=ccvm_code,
                                 doc_type=doc_type,
                                 files=files))


def has_ccvm(ccvm_code, doc_type):
    with lock:
        current_companies = get_control_file(FILES_BY_COMPANY_CTL, {})
        key = "{0}_{1}".format(ccvm_code, doc_type)
        return key in current_companies.keys()


def extract_company_files_from_page(
        driver, bs, doc_type="ITR", from_date=None):
    """
    Extract all the files to download from the listing HTML page

    :param driver: the panthomjs driver with the current page loaded. We use
                    the driver to navigate through the listing if needed
    :param bs: a BeautifulSoup object with the content of the listing page
    :param doc_type: the type of the files we are downloading
    :param from_date: if we are interested only in files presented after a
                    given date (newer files only)
    :return: a list of tuples with two components: the fiscal_period (date)
                and the protocol code for each file in the list
    """
    files = []

    # Extract the company name from the content
    company_name = re.search(RE_COMPANY_NAME, str(bs))[1].strip()

    # Extract the CNPJ from the content
    company_cnpj = re.search(RE_CNPJ, str(bs))[1].strip().lower()

    # Get the number of files we should expect to find for the company
    num_of_docs = int(re.search(RE_TOTAL_FILES, str(bs))[1])

    while True:

        # Get the number of files we can really get from the current page
        last_file_in_page = int(
            re.search(RE_LAST_FILE_IN_PAGE, str(bs))[2])

        # Obtain the table elements that contains information about files with
        # financial statements of the company
        all_tables = [tag.findParent("table") for tag in
                      bs.find_all(
                          text=re.compile("{} - ENET".format(doc_type)))
                      if tag.findParent("table")]

        # For each table we extract the files information of all the files
        # that belongs to a fiscal period after the from_date argument.
        for table in all_tables:
            link_tag = table.find('a', href=re.compile(RE_DOWNLOAD_FILE))
            if link_tag:
                fiscal_date = re.search(RE_FISCAL_DATE,str(table))[1]
                fiscal_date = date_parse(fiscal_date)

                delivery_date = re.search(RE_DELIVERY_DATE, str(table))[1]
                delivery_date = date_parse(delivery_date)

                # We only continue processing files from the HTML page
                # if are newer (deliver after) than the from_date argument.
                # We look for newer delivery files
                if from_date is not None and delivery_date <= from_date:
                    break

                version = re.search(RE_VERSION, str(table))[1]

                delivery_type = re.search(RE_DELIVERY_TYPE, str(table))[1]

                protocol = re.match(
                    RE_DOWNLOAD_FILE, link_tag.attrs['href'])[1]

                if not from_date or fiscal_date >= from_date:
                    files.append((fiscal_date, protocol, version,
                                  doc_type, delivery_type, delivery_date))
            else:
                _logger.debug("The file is not available in ITR format")

        if last_file_in_page == num_of_docs:
            break
        else:
            # Navigate to the next page
            element = driver.find_element_by_link_text("Próximos >>")
            element.click()

            # Wait until the page is loaded
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//form[@name='AIR']/table/*")))

            bs = BeautifulSoup(driver.page_source, "html.parser")

    return files


@Throttle(minutes=1, rate=20, max_tokens=20)
def obtain_company_files(
        phantomjs_path, ccvm, doc_type, from_date=None):
    """
    This function is responsible for get the relation of files to be
    processed for the company and start its download

    This function is being throttle allowing 20 downloads per minute
    """
    files = []
    driver = None

    _logger.debug("Starting to crawl company [{ccvm} - {doc_type}] ".
                  format(ccvm=ccvm,
                         doc_type=doc_type,
                         num_files=len(files)))

    try:
        driver = webdriver.PhantomJS(
            executable_path=phantomjs_path)

        encoded_args = urlencode(
            {'CCVM': ccvm, 'TipoDoc': 'C', 'QtLinks': "1000"})
        url = COMPANY_DOCUMENTS_URL.format(encoded_args)

        # Let's navigate to the url and wait until the reload is being done
        # We control that the page is loaded looking for an element with
        # id = "AIR" in the page
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'AIR')))

        # Once the page is ready, we can select the doc_type from the list
        # of documentation available and navigate to the results page
        # Select ITR files and Click
        element = driver.find_element_by_link_text(doc_type)
        element.click()

        # Wait until the page is loaded
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//form[@name='AIR']/table/*")))

        bs = BeautifulSoup(driver.page_source, "html.parser")
        files = extract_company_files_from_page(
            driver, bs, doc_type=doc_type, from_date=from_date)

        # Set checkpoint to the current ccvm code as a
        # company already processed
        update_companies_files_checkpoint(ccvm, doc_type, list(files))

        return files
    except NoSuchElementException as ex:
        _logger.debug("The company {ccvm} do not have {doc_type} documents".
              format(ccvm=ccvm, doc_type=doc_type))
        update_companies_files_checkpoint(ccvm, doc_type, [])
        return []
    except Exception as ex:
        _logger.exception("Unable to cral the documents for company {ccvm} "
                          "and {doc_type}".
              format(ccvm=ccvm, doc_type=doc_type))
        raise ex
    finally:
        _logger.debug("Finishing to crawl company [{ccvm} - {doc_type}] "
                      "files: [{num_files}]".
                      format(ccvm=ccvm,
                             doc_type=doc_type,
                             num_files=len(files)))
        if driver:
            _logger.debug("Closing the phantomjs driver for company "
                          "[{ccvm} - {doc_type}]".
                          format(ccvm=ccvm, doc_type=doc_type))
            driver.quit()


def crawl_company_files(
        phantomjs_path,
        doc_types,
        workers_num=10,
        from_date=None,
        force=False,
        include_companies=None):

    company_files_already_crawled = []
    companies_files = []
    pool = Pool(processes=workers_num)
    try:
        if force:
            if Path(FILES_BY_COMPANY_CTL).exists():
                shutil.move(
                    FILES_BY_COMPANY_CTL, "{}.bak".
                        format(FILES_BY_COMPANY_CTL))

        # Obtain the ccvm codes of all the listed companies
        ccvm_codes = []
        if not include_companies:
            with open("data/companies.csv", "r") as f:
                companies_codes = csv.DictReader(f)
                for company in companies_codes:
                    ccvm_codes.append(company["ccvm"])
        else:
            ccvm_codes.extend(include_companies)

        _logger.debug(
            "Processing the files of {} companies".format(len(ccvm_codes)))

        func_params = []
        for ccvm in ccvm_codes:
            for doc_type in doc_types:
                # We process only the informed companies, if there is
                #  any informed
                if include_companies and ccvm not in include_companies:
                    continue

                # Use checkpoint to check if the company was already crawled
                if not has_ccvm(ccvm, doc_type):
                    func_params.append([
                        phantomjs_path, ccvm, doc_type, from_date])
                else:
                    _logger.debug("Getting the files from the cache")
                    current_company_files = \
                        get_control_file(FILES_BY_COMPANY_CTL, {})
                    key = "{0}_{1}".format(ccvm, doc_type)
                    if key in current_company_files.keys():
                        company_files_already_crawled += \
                            current_company_files[key]

        call_results = pool.starmap(obtain_company_files, func_params)

        # Merge all the responses into one only list
        companies_files += list(
            itertools.chain.from_iterable(call_results))

        companies_files += company_files_already_crawled

        return get_control_file(FILES_BY_COMPANY_CTL, {})
    except TimeoutError:
        _logger.exception("Timeout error")
        raise
    finally:
        pool.close()
        pool.join()
        pool.terminate()
