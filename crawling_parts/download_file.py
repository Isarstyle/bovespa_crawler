# -*- coding: utf-8 -*
import logging
import re
import os
import pathlib
import shutil
import ntpath
import zipfile
import ssl
import csv
from multiprocessing.pool import Pool
from multiprocessing import Manager

import xmljson
from xml.etree.ElementTree import fromstring


from urllib.request import urlretrieve
from throttle import Throttle
from utils import get_control_file, put_control_file

DOWNLOAD_URL = "http://www.rad.cvm.gov.br/enetconsulta/" \
               "frmDownloadDocumento.aspx?CodigoInstituicao=1&" \
               "NumeroSequencialDocumento={}"

DOWNLOADED_FILES_CTL = "ctl/downloads.ctl"

manager = Manager()
lock = manager.Lock()

# Avoid check certificates
ssl._create_default_https_context = ssl._create_unverified_context

DOC_TYPE_ITR = "ITR"
DOC_TYPE_DFP = "DFP"
DOC_TYPES = [DOC_TYPE_ITR, DOC_TYPE_DFP]

DFP_FINANCIAL_INFO_INSTANT = "INSTANT"  # Individual
DFP_FINANCIAL_INFO_DURATION = "DURATION"  # Consolidated
FINANCIAL_INFO_TYPES = [DFP_FINANCIAL_INFO_INSTANT,
                        DFP_FINANCIAL_INFO_DURATION]

DFP_BALANCE_INVALID = "INVALID"
DFP_BALANCE_IF = "IF"  # ??
DFP_BALANCE_BPA = "ASSETS"  # Balanços Patrimoniais Activos
DFP_BALANCE_BPP = "LIABILITIES"  # Balanços Patrimoniais Passivo
DFP_BALANCE_DRE = "DRE"  # Demonstrativo de Resultados
DFP_BALANCE_DRA = "DRA"  # Demonstraçao do Resultado Abrangente
DFP_BALANCE_DFC_MD = "DFC_MD"  # Demonstrativo Fluxo de Caixa - Método Direto
DFP_BALANCE_DFC_MI = "DFC_MI"  # Demonstrativo Fluxo de Caixa - Método Indireto
DFP_BALANCE_DMPL = "DMPL"  # Demonstraçao das Mutaçoes do Patrimônio Líquido
DFP_BALANCE_DVA = "DVA"  # Demonstraçao Valor Adicionado

BALANCE_TYPES = [
    DFP_BALANCE_INVALID,    # 0
    DFP_BALANCE_IF,         # 1
    DFP_BALANCE_BPA,        # 2
    DFP_BALANCE_BPP,        # 3
    DFP_BALANCE_DRE,        # 4
    DFP_BALANCE_DRA,        # 5
    DFP_BALANCE_DFC_MD,     # 6
    DFP_BALANCE_DFC_MI,     # 7
    DFP_BALANCE_DMPL,       # 8
    DFP_BALANCE_DVA         # 9
]

RE_FILE_BY_ITR = r"^.*\.ITR"
RE_FILE_BY_DFP = r"^.*\.DFP"
RE_FILE_BY_XML = r"^.*\.XML"

FILE_DOCUMENT = "docs/Documento.xml"
FILE_CAPITAL_COMPOSITION = \
    "docs/ComposicaoCapitalSocialDemonstracaoFinanceiraNegocios.xml"
FILE_FINANCIAL_INFO = "docs/InfoFinaDFin.xml"

SHARES_NUMBER_ACCOUNTS = [
    ("1.89.01", "QuantidadeAcaoOrdinariaCapitalIntegralizado"),
    ("1.89.02", "QuantidadeAcaoPreferencialCapitalIntegralizado"),
    ("1.89.03", "QuantidadeTotalAcaoCapitalIntegralizado"),
    ("1.89.04", "QuantidadeAcaoOrdinariaTesouraria"),
    ("1.89.05", "QuantidadeAcaoPreferencialTesouraria"),
    ("1.89.06", "QuantidadeTotalAcaoTesouraria")
]

_logger = logging.getLogger("bovespa")


def quarter(date):
    return (date.month - 1) // 3 + 1


def convert_xml_into_json(file):
    with open(file) as f:
        xml_content = f.read().replace("\n", "")
        return xmljson.badgerfish.data(fromstring(xml_content))


def get_scales(available_files):
    """
    Obtain the Metric Scale and Quantity of Shares from the Document.xml file

    Where to find the values:
        xmldoc.child("Documento").child_value("CodigoEscalaMoeda")
        xmldoc.child("Documento").child_value("CodigoEscalaQuantidade")

    :param available_files: list of available files per name
    :return: the money scale and quantity of shares
    """
    data = convert_xml_into_json(available_files[FILE_DOCUMENT])

    money_scale = int(data["Documento"]["CodigoEscalaMoeda"]["$"])
    quant_scale = int(data["Documento"]["CodigoEscalaQuantidade"]["$"])
    money_scale = 1999 - money_scale * 999
    quant_scale = 1999 - quant_scale * 999

    return money_scale, quant_scale


def get_cap_composition_accounts(
        available_files, ccvm, fiscal_date, version):
    money_scale, quant_scale = get_scales(available_files)

    data = convert_xml_into_json(available_files[FILE_CAPITAL_COMPOSITION])

    accounts = []
    for acc_number, acc_name in SHARES_NUMBER_ACCOUNTS:
        account = {
            "ccvm": ccvm,
            "period": fiscal_date,
            "version": version,
            "balance_type": DFP_BALANCE_IF,
            "financial_info_type": DFP_FINANCIAL_INFO_DURATION,
            "number": acc_number,
            "name": acc_name
        }

        equity = data["ArrayOfComposicaoCapitalSocialDemonstracaoFinanceira"][
            "ComposicaoCapitalSocialDemonstracaoFinanceira"]

        if isinstance(equity, (list, tuple)):
            value = int(equity[len(equity) - 1][
                acc_name]["$"])
        else:
            value = int(equity[acc_name]["$"])

        account["value"] = int(value / quant_scale)
        accounts.append(account)

    return accounts


def get_financial_info_accounts(
        available_files, ccvm, fiscal_date, version, doc_type):
    accounts = []

    money_scale, quant_scale = get_scales(available_files)

    data = convert_xml_into_json(available_files[FILE_FINANCIAL_INFO])

    for account_info in data["ArrayOfInfoFinaDFin"]["InfoFinaDFin"]:
        acc_version = account_info["PlanoConta"]["VersaoPlanoConta"]
        account = {
            "ccvm": ccvm,
            "period": fiscal_date,
            "version": version,
            "balance_type": BALANCE_TYPES[
                int(acc_version["CodigoTipoDemonstracaoFinanceira"]["$"])],
            "financial_info_type": FINANCIAL_INFO_TYPES[
                int(acc_version["CodigoTipoInformacaoFinanceira"]["$"]) - 1],
            "number": str(account_info["PlanoConta"]["NumeroConta"]["$"]),
            "name": account_info["DescricaoConta1"]["$"],
        }

        if account["balance_type"] ==  DFP_BALANCE_DMPL:
            period = account_info[
                "PeriodoDemonstracaoFinanceira"][
                "NumeroIdentificacaoPeriodo"]["$"]
            if (doc_type == DOC_TYPE_DFP and period != 1) or \
                    (doc_type == DOC_TYPE_ITR and period != 4):
                continue

            # Shares outstanding
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Capital social integralizado"
            dmpl_account["value"] = float(account_info["ValorConta1"]["$"])
            accounts.append(dmpl_account)

            # Reserves
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Reservas de capital"
            dmpl_account["value"] = \
                float(account_info["ValorConta2"]["$"] / money_scale)
            accounts.append(dmpl_account)

            # Revenue reserves
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Reservas de lucro"
            dmpl_account["value"] = \
                float(account_info["ValorConta3"]["$"] / money_scale)
            accounts.append(dmpl_account)

            # Accrued Profit/Loss
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Lucros/Prejuízos acumulados"
            dmpl_account["value"] = \
                float(account_info["ValorConta4"]["$"] / money_scale)
            accounts.append(dmpl_account)

            # Accumulated other comprehensive income
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Outros resultados abrangentes"
            dmpl_account["value"] = \
                float(account_info["ValorConta5"]["$"] / money_scale)
            accounts.append(dmpl_account)

            # Stockholder's equity
            dmpl_account = dict(account)
            dmpl_account["comments"] = "Patrimônio Líquido"
            dmpl_account["value"] = \
                float(account_info["ValorConta6"]["$"] / money_scale)
            accounts.append(dmpl_account)
        else:
            if doc_type == DOC_TYPE_DFP:
                account["value"] = \
                    float(account_info["ValorConta1"]["$"]) / money_scale
            elif doc_type == DOC_TYPE_ITR:
                # Profit and Los (ASSETS or LIABILITIES)
                if account["balance_type"] in [
                        DFP_BALANCE_BPA, DFP_BALANCE_BPP]:
                    account["value"] = \
                        float(account_info["ValorConta2"]["$"]) / money_scale
                # Discounted Cash-flow (direct/indirect) and
                #   Value Added Demostration
                elif account["balance_type"] in [
                        DFP_BALANCE_DFC_MD, DFP_BALANCE_DFC_MI, DFP_BALANCE_DVA]:
                    account["value"] = \
                        float(account_info["ValorConta4"]["$"]) / money_scale
                else:
                    q = quarter(account["period"].date())
                    if q == 1:
                        account["value"] = \
                            float(account_info["ValorConta4"]["$"]) / money_scale
                    else:
                        account["value"] = \
                            float(account_info["ValorConta2"]["$"]) / money_scale
            accounts.append(account)

    return accounts


def load_account_details(available_files,
                         ccvm, fiscal_date, version, doc_type):
    _logger.debug("Loading accounts for: "
                  "{ccvm} - {fiscal_date} - {version} - {doc_type}".format(
        ccvm=ccvm, fiscal_date=fiscal_date,
        version=version, doc_type=doc_type))

    try:
        accounts = get_cap_composition_accounts(
            available_files, ccvm, fiscal_date, version)
        accounts.extend(get_financial_info_accounts(
            available_files, ccvm, fiscal_date, version, doc_type))

        company_fin_info = {
            "ccvm": ccvm, "period": fiscal_date, "version": version}

        column_names = {
            "ccvm": "Company Bovespa Code",
            "period": "Date of the financial data",
            "version": "Delivered version"}

        for account in accounts:
            company_fin_info[account["number"]] = account["value"]
            column_names[account["number"]] = account["name"]

        return company_fin_info, column_names
    except Exception as ex:
        _logger.exception("Error extracting account info for: "
                          "{ccvm} - {fiscal_date} - {version} - {doc_type}".
            format(ccvm=ccvm,
                   fiscal_date=fiscal_date,
                   version=version,
                   doc_type=doc_type))
        raise ex


def update_download_files_checkpoint(ccvm_code, files=None):
    with lock:
        if files:
            current_companies = get_control_file(DOWNLOADED_FILES_CTL, {})
            company_files = current_companies.setdefault(ccvm_code, [])
            if isinstance(files, (list, tuple)):
                for file in files:
                    if file not in company_files:
                        company_files.extend(files)
            else:
                if files not in company_files:
                    company_files.append(files)
            current_companies[ccvm_code] = company_files
            put_control_file(DOWNLOADED_FILES_CTL, current_companies)


def delete_all(path):
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            else:
                shutil.rmtree(file_path)
        except Exception as e:
            raise Exception(
                "Unable to delete the files in folder [{0}]".format(path))


def extract_zip(file, dest_path):
    with zipfile.ZipFile(file, "r") as zip_ref:
        zip_ref.extractall(dest_path)

    files_ref = []
    for the_file in os.listdir(dest_path):
        files_ref.append(os.path.join(dest_path, the_file))

    return files_ref


def extract_file_content(cache_folder,
                         ccvm,
                         fiscal_date,
                         version,
                         doc_type,
                         file):
    """Extract the files from the ENER zip file and the ITR/DFP inside of it,
    and collect all the XML files
    """

    filename = "CCVM_{0}_{1:%Y%m%d}_{2}.{3}".format(
        ccvm, fiscal_date, version.replace(".", ""), doc_type)

    dest_path = pathlib.Path(cache_folder, ccvm, "exploded", filename)
    if not dest_path.exists():
        dest_path.mkdir(parents=True, exist_ok=True)

    # Clean the folder to explode the new content
    delete_all(dest_path)

    files_ref = extract_zip(file, dest_path)

    available_files = {}

    if doc_type in ["ITR", "DFP"]:
        for the_file in files_ref:
            if re.match(RE_FILE_BY_XML, the_file, re.IGNORECASE):
                filename = ntpath.basename(the_file)
                available_files[filename] = the_file
            elif re.match(RE_FILE_BY_ITR, the_file, re.IGNORECASE):
                itr_dest_folder = "{0}/itr_content/".\
                    format(dest_path)
                itr_files = extract_zip(the_file, itr_dest_folder)
                for itr_file in itr_files:
                    filename = ntpath.basename(itr_file)
                    available_files["docs/{}".format(filename)] = itr_file
            elif re.match(RE_FILE_BY_DFP, the_file, re.IGNORECASE):
                dfp_dest_folder = "{0}/dfp_content/".\
                    format(dest_path)
                dfp_files = extract_zip(the_file, dfp_dest_folder)
                for dfp_file in dfp_files:
                    filename = ntpath.basename(dfp_file)
                    available_files["docs/{}".format(filename)] = dfp_file

    return available_files


def generate_dataset(results):
    headers = {"ccvm": "Company Bovespa Code",
               "period": "Date of the financial data",
               "version": "Delivered version"}
    docs = []
    for company_info, company_headers, file in results:
        headers.update(company_headers)
        docs.append(company_info)

    with open("data/dataset.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers.keys())
        writer.writeheader()
        writer.writerows(docs)

    dictionary_headers = ["Field", "Description"]
    with open("data/dictionary.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=dictionary_headers)
        writer.writeheader()
        for field_name, field_desc in headers.items():
            writer.writerow({"Field": field_name,
                             "Description": field_desc})


@Throttle(minutes=1, rate=20, max_tokens=20)
def download_file(
        cache_folder,
        ccvm, fiscal_date, version, doc_type, protocol,
        force_download=True):
    """
    This function is responsible for download the financial statements of a
    public company based on a protocol code.

    This function is being throttle allowing 20 downloads per minute

    :param ccvm: the unique code of the company in bovespa
    :param fiscal_period: the fiscal period of the financial statements
                          related to the protocol we want to download
    :param doc_type: the type of file we are downloading.
                     It will be the extension of the future local file
    :param protocol: the code of the file to be download
    :param force_download: we only download the file if it's not already
                           present in the local cache
    :param cache_folder: the folder to place the company files (local cache)
    :param force_download: if we want to download the company file no matter
                            if it already exists in the cache
    """
    filename = "CCVM_{0}_{1:%Y%m%d}_{2}.{3}".format(
        ccvm, fiscal_date, version.replace(".", ""), doc_type)

    file = pathlib.Path(cache_folder, ccvm, filename)
    if not file.exists():
        file.parent.mkdir(parents=True, exist_ok=True)

    if force_download or not file.exists():
        urlretrieve(DOWNLOAD_URL.format(protocol), filename=filename)
        shutil.move(filename, file)

    update_download_files_checkpoint(ccvm, str(file))

    financial_files = extract_file_content(
        cache_folder, ccvm, fiscal_date, version, doc_type, file)

    company_info, headers_info = load_account_details(
        financial_files, ccvm, fiscal_date, version, doc_type)

    return company_info, headers_info, file


def download_files(cache_folder,
                   files_per_ccvm_and_doc_type,
                   doc_types,
                   workers_num=10,
                   force_download=False,
                   include_companies=None):

    pool = Pool(processes=workers_num)
    try:
        func_params = []
        for key, files in files_per_ccvm_and_doc_type.items():

            ccvm, doc_type = key.split("_")

            # We process only the informed companies, if there is any informed
            if include_companies and ccvm not in include_companies:
                continue

            for (fiscal_date, protocol, version,
                 doc_type, delivery_type, delivery_date) in files:

                filename = "CCVM_{0}_{1:%Y%m%d}_{2}.{3}".format(
                    ccvm, fiscal_date, version.replace(".",""), doc_type)

                file = pathlib.Path(cache_folder, ccvm, filename)

                func_params.append([
                    cache_folder, ccvm, fiscal_date, version,
                    doc_type, protocol, force_download])

            _logger.debug("Downloading {} files...".format(len(func_params)))
            call_results = pool.starmap(download_file, func_params)

            generate_dataset(call_results)
    except TimeoutError:
        _logger.exception("Timeout error")
        raise
    finally:
        pool.close()
        pool.join()
        pool.terminate()
