# -*- coding: utf-8 -*
import logging
import logging.config
import argparse
from pathlib import Path

from utils import mk_datetime

from crawling_parts.listed_companies import crawl_listed_companies
from crawling_parts.company_files import crawl_company_files
from crawling_parts.download_file import download_files

logging.config.fileConfig("log_config.conf")
_logger = logging.getLogger("bovespa")


def crawl(cache_folder=None,
          from_date=None,
          workers_num=10,
          phantomjs_path=None,
          force_crawl_listed_companies=False,
          force_crawl_company_files=False,
          include_companies=None):

    # Force the creation of the cache folder if it does not exists
    cache_path = Path(cache_folder)
    if not cache_path.exists():
        cache_path.mkdir(parents=True, exist_ok=True)

    # Crawl the companies that are and have been registered into the
    # stock market in Brazil. These will be the companies we will crawl
    crawl_listed_companies(phantomjs_path,
                           workers_num=workers_num,
                           force=force_crawl_listed_companies)

    # Let's crawl the files information available for each company
    # and for each period.
    # The company_files is a combination of:
    #       financial_period + protocol + doc_type.
    # The protocol identifies the file to be downloaded
    companies_files = crawl_company_files(
        phantomjs_path,
        ["ITR", "DFP"],
        workers_num=workers_num,
        from_date=from_date,
        force=force_crawl_company_files,
        include_companies=include_companies)

    # Let's download the files with the financial statements of the companies
    download_files(cache_folder,
                   companies_files,
                   ["ITR", "DFP"],
                   workers_num=workers_num,
                   force_download=force_crawl_company_files,
                   include_companies=include_companies)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Financial Statements Extractor from Bovespa")

    parser.add_argument("--from-date",
                        type=mk_datetime,
                        required=False,
                        dest="from_date",
                        help="Extract only the data after an specific date. "
                             "(ex: 2018-01-01)")
    parser.add_argument("--cache-folder",
                        type=str,
                        required=False,
                        default="./crawler_cache",
                        dest="cache_folder",
                        help="The folder we want to use to save the"
                             " downloaded files. "
                             "(ex: /data/crawlers/bovespa")
    parser.add_argument("--workers-num",
                        action='store',
                        default=10,
                        type=int,
                        required=False,
                        dest="workers_num",
                        help="The number of parallel threads crawling."
                             "(ex: 20")
    parser.add_argument("--phantomjs-path",
                        action='store',
                        required=True,
                        dest="phantomjs_path",
                        help="The path where we can found the PanthomJS "
                             "librery installed."
                             "(ex: /phantomjs-2.1.1-macosx/bin/phantomjs)")
    parser.add_argument("--force-crawl-listed-companies",
                        action='store_true',
                        required=False,
                        dest="force_crawl_listed_companies",
                        help="If we want to bypass the checkpoint control and"
                             " crawl the basic data of all the open companies "
                             " in bovespa."
                             "(ex: --force-crawl-listed-companies")
    parser.add_argument("--force-crawl-company-files",
                        action='store_true',
                        required=False,
                        dest="force_crawl_company_files",
                        help="If we want to bypass the checkpoint control and "
                             "crawl the company files since the begining."
                             "(ex: --force-crawl-company-files")
    parser.add_argument("--include-companies",
                        action='store',
                        nargs='*',
                        required=False,
                        dest="include_companies",
                        help="If we want to focus only on a specific "
                             "companies."
                             "(ex: 35 94 1384")

    args, unknown = parser.parse_known_args()

    try:
        crawl(**vars(args))
    except Exception as ex:
        _logger.exception("Exception running the crawler. Arguments: {}".
                          format(vars(args)))
        exit(2)