from pymongo import MongoClient
from  pymongo.collection import Collection 
from unidecode import unidecode
from bson.objectid import ObjectId
from thefuzz import fuzz
from thefuzz import process
from mohan.Similarity import Similarity
from joblib import Parallel, delayed

#global variables for this cut, required to be edited
yuku_db = "yuku"
yuku_grcol = "gruplac_production_data"
yuku_cvcol = "cvlac_stage"

# corte del author >= 65
# corte del paper sin author >= 95 con author >=90
author_thd = 65
paper_thd_low = 90
paper_thd_high = 94


categories_art = ['ART-00', 'ART-ART_A1', 'ART-ART_A2', 'ART-ART_B', 'ART-ART_C', 'ART-ART_D', 'ART-GC_ART']
categories_books = ['PE-PE', 'PID-00', 'PIC-00', 'PID-PID', 'PE-00', 'PF-00', 'PID-EX', 'PIC-PIC', 'PF-PF', 'PF-EX']

categories = categories_art + categories_books
pipeline = [
    {'$match': {'id_tipo_pd_med': {'$in': categories}}},
    {'$group': {'_id': '$id_producto_pd', 'originalDoc': {'$first': '$$ROOT'}}}, ## los productos se repiten por convocatoria
    {'$replaceRoot': {'newRoot': '$originalDoc'}}
]

pipeline_zeros = [
    {'$match': {'id_tipo_pd_med': {'$in': categories}}},
    {'$match': {'id_producto_pd':"0000000000"}}
]

def str_normilize(word:str)->str:
    """
    Basic normalization function for strings

    Parameters:
    ----------
    word: str
        word to normalize
    
    Returns:
    -------
    str
        normalized word
    """
    return unidecode(word).lower().strip().replace(".","")

def check_work(openalex_in:Collection,openalex_out:Collection,title_work:str,authors:list, response:dict)->int:
    """
    function to check if the work should be inserted,
    checks if the title and author are similar enough, if so inserts the work.
    If there are not authors, the title must have at least 30 characters to be considered
    and the thresholds are different.

    NOTE: if the work is already in the collection  mongo will raise an exception, but it is not a problem.
    in parallel processing this can happen, but if the work is already inserted will be just ignored.

    Parameters:
    ----------
    openalex_in: pymongo.collection.Collection
        collection with the openalex data
    openalex_out: pymongo.collection.Collection
        collection where the data will be inserted (colombia cut)
    title_work: str
        title of the work to insert
    authors: list
        list of authors of the work
    response: dict
        response from the elasticsearch query

    Returns:
    -------
    int
        1 if the work was inserted, 0 otherwise
    """
    author_found = False
    if authors:
        if authors[0] != "":
            _authors=[]
            for author in response["_source"]["authors"]:
                _authors.append(str_normilize(author))
            scores = process.extract(str_normilize(authors[0]), _authors,scorer=fuzz.partial_ratio)
            for score in scores:
                if score[1]>= author_thd:
                    author_found=True
                    break
            #print(scores)
    if response["_source"]["title"]:
        score  = fuzz.WRatio(str_normilize(title_work),str_normilize(response["_source"]["title"]))
        if author_found:
            if score >= paper_thd_low:
                work = openalex_out["works"].find_one({"_id":ObjectId(response["_id"])})
                if work is None:
                    oa_work = openalex_in["works"].find_one({"_id":ObjectId(response["_id"])})
                    try:
                        openalex_out["works"].insert_one(oa_work)
                    except Exception as e:
                        print(e)
                        print("Inserting multiples records in parallel can cause this, but it is not a problem.")
                        print("Continuing")
                return 1
            else:
                return 0
        else:
            if score >= paper_thd_high:
                work = openalex_out["works"].find_one({"_id":ObjectId(response["_id"])})
                if work is None:
                    oa_work = openalex_in["works"].find_one({"_id":ObjectId(response["_id"])})
                    try:
                        openalex_out["works"].insert_one(oa_work)
                    except Exception as e:
                        if oa_work is None:
                            print("work was not found in elasticsearch, maybe es data have to be updated")
                        print(e)
                        print("Inserting multiples records in parallel can cause this, but it is not a problem.")
                        print("Continuing")
 
                return 1
            else:
                return 0
    else:
        print(response)
    return 0
    
def process_one(openalex_in:Collection,openalex_out:Collection,cv_col:Collection,s:Similarity,work:dict)->int:
    """
    function to process one work

    Parameters:
    ----------
    openalex_in: pymongo.collection.Collection
        collection with the openalex data
    openalex_out: pymongo.collection.Collection
        collection where the data will be inserted (colombia cut)
    cv_col: pymongo.collection.Collection
        collection with the cvlac to search author information
    s: Similarity
        similarity object to search works with elasticsearch
    work: dict
        work to process from grouplac production data
    
    Returns:
    -------
    int
        1 if the work was inserted, 0 otherwise
    """
    title_work = work.get("nme_producto_pd","")
    authors = []
    if 'id_persona_pd' in work.keys():
        if work["id_persona_pd"]:
            author = cv_col.find_one({"id_persona_pr":work["id_persona_pd"]},{"datos_generales.Nombre":1})
            if author:
                authors.append(author["datos_generales"]["Nombre"])
    if not authors and len(title_work) < 30:
        return 0
    if title_work != "":
        responses = s.search_work(
            title=title_work,
            source="",
            year="0",
            authors=authors,
            volume="",
            issue="",
            page_start="",
            page_end="",
            use_es_thold=True,
            es_thold=0,
            hits=20
        )
        if responses:
            for responce in responses:
                out = check_work(openalex_in,openalex_out,title_work,authors,responce)
                if out:
                    return 1      
    return 0

#id_producto_pd no es único por que cambia para cada autor (los titulos pueden ser iguales)
#tocar chequear si el producto ya está en la base de datos
#si ya está no se inserta
def colombia_cut_minciencias(db_in:str,db_out:str,es_index:str, jobs:int=20, backend:str="threading")->None:
    c = MongoClient()
    gr_col = c[yuku_db][yuku_grcol]
    paper_cursor = gr_col.aggregate(pipeline, allowDiskUse=True)
    data = list(paper_cursor)
    paper_cursor = gr_col.aggregate(pipeline_zeros, allowDiskUse=True)
    data.extend(list(paper_cursor))
    openalex_in = c[db_in]
    openalex_out = c[db_out]
    cv_col = c[yuku_db][yuku_cvcol]
    s = Similarity(es_index,es_uri= "http://localhost:9200",es_auth = ('elastic', 'colav'))
    results = Parallel(n_jobs=jobs,backend="threading",verbose=10,batch_size=4)(delayed(process_one)(openalex_in,openalex_out,cv_col,s,work) for work in data)