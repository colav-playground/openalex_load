from pymongo import MongoClient
from kahi_impactu_utils.Utils import doi_processor
import pandas as pd
from pandas import isna
from joblib import Parallel, delayed
from bs4 import BeautifulSoup
import re

###############################
#Global variables, please edit#
############################### 

#Google Scholar
db_gs="scholar_colombia_2024"
col_gs="data"

#Scienti
dbs_sci = ["scienti_udea_2024","scienti_uec_2024","scienti_unaula_2024","scienti_univalle_2024"]
col_sci="product"

# Sc
db_sc = "scopus_colombia"
col_sc = "stage"

#WS
db_wos = "wos_colombia"
col_wos = "stage"

#DAM
db_dam = "yuku_2025_2"
col_dam = "cvlac_stage_raw"

#DSpace
db_dspace="oxomoc_colombia"
dspace_pipeline = [
  {
    "$project": {
      "doi": {
        "$filter": {
          "input": {
            "$cond": [
              { "$isArray": "$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field" },
              "$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field",
              [ "$OAI-PMH.GetRecord.record.metadata.dim:dim.dim:field" ]
            ]
          },
          "as": "field",
          "cond": {
            "$and": [
              { "$eq": ["$$field.@element", "identifier"] },
              { "$eq": ["$$field.@qualifier", "doi"] }
            ]
          }
        }
      },
        "_id":0
    }
      
  }
]


# CIARP Institutions
# CIARP Univalle, no tiene ningún doi
ciarp_files=["/storage/kahi_data/kahi_data/staff/formato_CIARP_UDEA_2024_11.xlsx"]

# DAM
def extract_doi_candidates_from_html(html: str) -> list[str]:
  '''
  Extract raw DOI-like strings from the HTML content
  '''
  candidate_pattern = r"10\.\d{3,}/[^\s\"'<]+"
  candidates = re.findall(candidate_pattern, html, flags=re.IGNORECASE)
  return candidates

def extract_valid_dois(html: str) -> list[str]:
  '''
  Return unique, normalized DOIs found in the HTML using doi_processor
  '''
  candidates = extract_doi_candidates_from_html(html)
  valid_dois = set()

  for cand in candidates:
      doi_url = doi_processor(cand)
      if doi_url:
          valid_dois.add(doi_url)

  return valid_dois

def process_doi(c:MongoClient, doi:str,db_in:str,db_out:str)->None:
   work=c[db_in]["works"].find_one({"doi":doi})
   if work:
        found = c[db_out]["works"].count_documents({"id":work["id"]})
        if found == 0:
            c[db_out]["works"].insert_one(work)

def colombia_cut_dois( db_in:str,db_out:str, jobs:int=72, backend="threading")->None:
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

    #DSpace
    db=c[db_dspace]
    collections = db.list_collection_names(filter= {"name": {"$regex": r"^dspace.*records$"}})
    for collection in collections:
        print(f"INFO: processing {collection}")
        cursor=db[collection].aggregate(dspace_pipeline)
        for doc in cursor:
            if doc["doi"] == None:
                continue
            for raw_doi in doc["doi"]:
                if raw_doi:
                    if not "#text" in raw_doi:
                        continue
                    dois.append(raw_doi["#text"])

    # puntaje
    for ciarp_file in ciarp_files:
        data = pd.read_excel(ciarp_file)
        dois.extend(data["doi"].dropna().values.tolist())

    # DAM
    cursor = c[db_dam][col_dam].find()
    raw = Parallel(n_jobs=-1, verbose=10)(
        delayed(extract_valid_dois)(item["html"])
        for item in cursor
    )
    _dois = [d for sub in raw for d in sub if d]
    _dois = list(set(_dois))
    dois.extend(_dois)
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
                pdois.append(pdoi)
    pdois=list(set(pdois)-set(oa_dois_inserted)) #removing already cutted dois
    print(f"INFO: dois found = {len(pdois)}")
    out = Parallel(n_jobs=jobs,backend=backend,verbose=10,batch_size=4)(delayed(process_doi)(c,doi,db_in,db_out) for doi in pdois)
