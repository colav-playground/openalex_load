from pymongo import MongoClient
from kahi_impactu_utils.Utils import doi_processor
import pandas as pd
from pandas import isna
from joblib import Parallel, delayed
###############################
#Global variables, please edit#
############################### 
oa_doi_prefix="https://doi.org/"

#Google Scholar
db_gs="scholar_colombia_2024"
col_gs="data"

#Scienti
dbs_sci = ["scienti_udea_2023","scienti_udea_2023","scienti_unaula_2023","scienti_univalle_2023"]
col_sci="product"

# Sc
db_sc = "scopus_colombia"
col_sc = "stage"

#WS
db_wos = "wos_colombia"
col_wos = "stage"

# puntaje
ranking_file="/storage/kahi_data/kahi_data/staff/produccion 2018-2023.xlsx"


def process_doi(c:MongoClient, doi:str,db_in:str,db_out:str)->None:
   work=c[db_in]["works"].find_one({"doi":doi})
   if work:
       c[db_out]["works"].insert_one(work)

def colombia_cut_dois( db_in:str,db_out:str, jobs:int=20)->None:
    c=MongoClient()
    dois = []

    #Google Scholar
    data = list(c[db_gs][col_gs].find({"doi":{"$ne":"","$exists":1}},{"doi":1,"_id":0}))
    for doi in data:
        try:
            dois.append(doi['doi'])
        except:
            print(doi)

    #Scienti
    for db in dbs_sci:
        data = list(c[db][col_sci].find({"TXT_DOI":{"$ne":None,"$ne":""}},{"TXT_DOI":1,"_id":0}))
        for doi in data:
            dois.append(doi["TXT_DOI"])

    #Sc
    data = list(c[db_sc][col_sc].find({"DOI":{"$ne":None,"$ne":""}},{"DOI":1,"_id":0}))
    for doi in data:
        if not isna(doi["DOI"]):
            dois.append(doi["DOI"])

    #WS
    data = list(c[db_wos][col_wos].find({"DI":{"$ne":""}},{"DI":1,"_id":0}))
    for doi in data:
        dois.append(doi["DI"])

    # puntaje
    data = pd.read_excel(ranking_file)
    dois.extend(data["DOI"].dropna().values.tolist())

    # dois from already cutted colombian data (taking it from db_out)
    data = list(c[db_out]["works"].find({"doi":{"$ne":None}}))
    oa_dois_inserted=[]
    for doi in data:
        oa_dois_inserted.append(doi["doi"])

    pdois=[]
    for doi in dois:
        if doi is not None:
            pdoi=doi_processor(doi)
            if pdoi:
                pdois.append(oa_doi_prefix+pdoi)
    pdois=list(set(pdois)-set(oa_dois_inserted)) #removing already cutted dois
    out = Parallel(n_jobs=jobs,backend="threading",verbose=10,batch_size=4)(delayed(process_doi)(c,doi,db_in,db_out) for doi in pdois)
