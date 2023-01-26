import requests, json
from requests.auth import HTTPDigestAuth
import sys
from atlasConfig import atlasConf as conf
from pymongo import MongoClient

# def getAllRSClusterPrimaries(processURL, auth): 
#     response = requests.get(processURL, auth = auth)
#     rsClusterPrimaries = []
    
#     for d in response.json()["results"]:
#         if d["typeName"] == "REPLICA_PRIMARY":
#             rsClusterPrimaries.append({d["replicaSetName"]: d["hostname"]})
    
#     return rsClusterPrimaries

def getSearchIndexesForCollection(allSearchIndexesForCollectionURL, auth):
    response = requests.get(allSearchIndexesForCollectionURL, auth = auth)

    return response.json()

def createSearchIndex(createSearchIndexURL, data, auth):
    headers = {"Content-Type": "application/json"}
    response = requests.post(createSearchIndexURL, headers=headers, json=data, auth = auth)

    return response.json()


if __name__ == "__main__":
    auth = HTTPDigestAuth(conf["SRC_ATLAS_PUBLIC_KEY"], conf["SRC_ATLAS_PRIVATE_KEY"])

    srcProjectURL = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{conf['SRC_PROJECT_ID']}"
    srcClusterURL = f"{srcProjectURL}/clusters/{conf['SRC_CLUSTER_NAME']}"
    tgtProjectURL = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{conf['TGT_PROJECT_ID']}"
    tgtClusterURL = f"{tgtProjectURL}/clusters/{conf['TGT_CLUSTER_NAME']}"

    srcClient = MongoClient(conf["SRC_CONN_STR"])
    srcDbs = srcClient.list_database_names()

    # allSrcSearchIndexes = []
    # counter = 0
    # for dbName in srcDbs:
    #     db = srcClient[dbName]
    #     # print(f"{dbName} \n\t {db.list_collection_names()}")
    #     for collName in db.list_collection_names():
    #         allSearchIndexesForCollectionURL = f"{clusterURL}/fts/indexes/{dbName}/{collName}"
    #         collSearchIndexes = getSearchIndexesForCollection(allSearchIndexesForCollectionURL, auth)
    #         if len(collSearchIndexes) > 0:
    #             for index in collSearchIndexes:
    #                 allSrcSearchIndexes.append(index)
            
    #         counter += 1
    #         print(f"Analyzed {counter} collections on source")

    # print(allSrcSearchIndexes)

    createSearchIndexURL = f"{tgtClusterURL}/fts/indexes"

    allSrcSearchIndexes = [{'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '634681f214b2a2757c45cc81', 'mappings': {'dynamic': True}, 'name': 'gutenoogleberg_en_default', 'status': 'STEADY', 'storedSource': True, 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '63468bb74ffaa00cf6fe00c7', 'mappings': {'dynamic': True}, 'name': 'default', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '63469952c572ed0e233d7ce6', 'mappings': {'dynamic': False}, 'name': 'default1', 'status': 'STEADY', 'storedSource': True, 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '63478d7f6605ea12fbadbb5b', 'mappings': {'dynamic': False, 'fields': {'creator': {'store': False, 'type': 'string'}}}, 'name': 'gutenoogleberg_en_only_index_creator_store_false', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '63478f305448b97134230ca0', 'mappings': {'dynamic': False, 'fields': {'creator': {'store': True, 'type': 'string'}}}, 'name': 'gutenoogleberg_en_only_index_creator_store_true', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '634791df6605ea12fbae1863', 'mappings': {'dynamic': False, 'fields': {'creator': {'store': True, 'type': 'string'}}}, 'name': 'gutenoogleberg_en_only_index_creator_store_true_store_original_true', 'status': 'STEADY', 'storedSource': {'include': ['creator']}, 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '6347cd383971364cd0c207fd', 'mappings': {'fields': {'content': {'type': 'string'}, 'creator': {'type': 'string'}, 'subject': {'type': 'string'}, 'title': {'type': 'string'}}}, 'name': 'gutenoogleberg_en_single_search_box', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'en', 'database': 'gutenoogleberg', 'indexID': '6347fcbe790d47124711c09d', 'mappings': {'dynamic': False, 'fields': {'creator': [{'type': 'stringFacet'}, {'analyzer': 'lucene.standard', 'multi': {'keywordAnalyzer': {'analyzer': 'lucene.keyword', 'type': 'string'}}, 'type': 'string'}], 'detectedlanguagelingua': {'type': 'stringFacet'}, 'releasedate': {'type': 'dateFacet'}, 'subject': {'type': 'string'}, 'title': {'type': 'string'}}}, 'name': 'gutenoogleberg_en_single_search_box_facets', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'listingsAndReviews', 'database': 'sample_airbnb', 'indexID': '63485773c289982f83fc67b8', 'mappings': {'dynamic': False, 'fields': {'address': {'fields': {'market': {'analyzer': 'lucene.keyword', 'searchAnalyzer': 'lucene.keyword', 'type': 'string'}}, 'type': 'document'}, 'reviews': {'dynamic': True, 'type': 'embeddedDocuments'}}}, 'name': 'sample_airbnb_listingsAndReviews_address_reviews', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'planets', 'database': 'sample_guides', 'indexID': '63486947c505a434f7e00cc0', 'mappings': {'dynamic': False, 'fields': {'name': {'type': 'string'}}}, 'name': 'sample_guides_planets_name', 'status': 'STEADY', 'synonyms': [{'analyzer': 'lucene.standard', 'name': 'planetTerms', 'source': {'collection': 'synonyms'}}]}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '63451616873e2c33eba8b0e9', 'mappings': {'dynamic': False, 'fields': {'title': {'foldDiacritics': True, 'maxGrams': 7, 'minGrams': 3, 'tokenization': 'edgeGram', 'type': 'autocomplete'}}}, 'name': 'default', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '63455ecddd3822357836e923', 'mappings': {'dynamic': False, 'fields': {'title': [{'foldDiacritics': True, 'maxGrams': 7, 'minGrams': 3, 'tokenization': 'nGram', 'type': 'autocomplete'}, {'type': 'string'}]}}, 'name': 'default_ngram', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '63455effdd3822357836f14c', 'mappings': {'dynamic': False, 'fields': {'title': {'foldDiacritics': True, 'maxGrams': 7, 'minGrams': 3, 'tokenization': 'rightEdgeGram', 'type': 'autocomplete'}}}, 'name': 'default_regram', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '634646837613d166ec0d5dfe', 'mappings': {'dynamic': True}, 'name': 'wild', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '6347d665bb994510e4f32221', 'mappings': {'dynamic': False, 'fields': {'genres': {'type': 'stringFacet'}, 'released': {'type': 'date'}, 'year': {'type': 'numberFacet'}}}, 'name': 'idx_gyr', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '63486394e85714661d5b9c67', 'mappings': {'dynamic': False, 'fields': {'title': {'analyzer': 'lucene.english', 'type': 'string'}}}, 'name': 'synTransport', 'status': 'STEADY', 'synonyms': [{'analyzer': 'lucene.english', 'name': 'transportSynonyms', 'source': {'collection': 'transport_synonyms'}}, {'analyzer': 'lucene.english', 'name': 'attireSynonyms', 'source': {'collection': 'attire_synonyms'}}]}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '637c876eb705f24acf514433', 'mappings': {'dynamic': False, 'fields': {'title': [{'foldDiacritics': False, 'maxGrams': 7, 'minGrams': 3, 'tokenization': 'edgeGram', 'type': 'autocomplete'}]}}, 'name': 'auto_maxExpansion', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'movies', 'database': 'sample_mflix', 'indexID': '63cb61f9ebb12454e5f75eae', 'mappings': {'dynamic': True}, 'name': 'default1', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'sales', 'database': 'sample_supplies', 'indexID': '63484defa8f7bd18fa7900f3', 'mappings': {'dynamic': True, 'fields': {'items': {'dynamic': True, 'type': 'embeddedDocuments'}, 'purchaseMethod': {'type': 'stringFacet'}}}, 'name': 'default', 'status': 'STEADY', 'synonyms': []}, {'analyzer': 'lucene.english', 'collectionName': 'quotes', 'database': 'search', 'indexID': '6344ea510d06101159ac28ae', 'mappings': {'dynamic': True}, 'name': 'default', 'searchAnalyzer': 'lucene.english', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'coll', 'database': 'search', 'indexID': '6344106a73f52a069fc3d1df', 'mappings': {'dynamic': True}, 'name': 'default', 'status': 'STEADY', 'synonyms': []}, {'analyzer': 'htmlStrippingAnalyzer', 'analyzers': [{'charFilters': [{'ignoredTags': ['a'], 'type': 'htmlStrip'}], 'name': 'htmlStrippingAnalyzer', 'tokenFilters': [], 'tokenizer': {'type': 'standard'}}], 'collectionName': 'coll', 'database': 'search', 'indexID': '63441229e5e9f72c2642313d', 'mappings': {'dynamic': True}, 'name': 'IDXcf_html', 'searchAnalyzer': 'htmlStrippingAnalyzer', 'status': 'STEADY', 'synonyms': []}, {'analyzer': 'icuNormalizingAnalyzer', 'analyzers': [{'charFilters': [{'type': 'icuNormalize'}], 'name': 'icuNormalizingAnalyzer', 'tokenFilters': [], 'tokenizer': {'type': 'whitespace'}}], 'collectionName': 'coll', 'database': 'search', 'indexID': '634414417192db0e7ab93b1f', 'mappings': {'dynamic': True}, 'name': 'IDX_cf_icu', 'searchAnalyzer': 'icuNormalizingAnalyzer', 'status': 'STEADY', 'synonyms': []}, {'analyzer': 'mappingAnalyzer', 'analyzers': [{'charFilters': [{'mappings': {'\\': '/'}, 'type': 'mapping'}], 'name': 'mappingAnalyzer', 'tokenFilters': [], 'tokenizer': {'type': 'keyword'}}], 'collectionName': 'coll', 'database': 'search', 'indexID': '634416660fb5fd7e233bd367', 'mappings': {'dynamic': True}, 'name': 'IDX_cf_map', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'person', 'database': 'search', 'indexID': '6342ce724b1d5b3f1b8dc059', 'mappings': {'dynamic': True}, 'name': 'default', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'person', 'database': 'search', 'indexID': '6343074cea9010423d6c9719', 'mappings': {'dynamic': False, 'fields': {'first_name': {'type': 'string'}, 'last_name': {'type': 'string'}, 'middle_name': {'type': 'string'}}}, 'name': 'pNames', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'person', 'database': 'search', 'indexID': '6343adce2a72dd1ae263b7ab', 'mappings': {'dynamic': False, 'fields': {'address': {'fields': {'loc': {'type': 'geo'}}, 'type': 'document'}, 'first_name': {'type': 'string'}, 'last_name': {'type': 'string'}, 'middle_name': {'type': 'string'}}}, 'name': 'plNames', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'person', 'database': 'search', 'indexID': '6344f89ad4032850be5cf0d4', 'mappings': {'dynamic': False, 'fields': {'phone': {'type': 'string'}}}, 'name': 'idx_phone', 'status': 'STEADY', 'synonyms': []}, {'analyzers': [{'charFilters': [], 'name': 'areaCodeAnalyzer', 'tokenFilters': [], 'tokenizer': {'group': 1, 'pattern': '^\\+1(\\d{3})\\d{7}', 'type': 'regexCaptureGroup'}}], 'collectionName': 'person', 'database': 'search', 'indexID': '634508803b96d838e091ec18', 'mappings': {'dynamic': False, 'fields': {'address': {'fields': {'loc': {'type': 'geo'}}, 'type': 'document'}, 'first_name': {'type': 'string'}, 'last_name': {'type': 'string'}, 'middle_name': {'type': 'string'}, 'phone': {'analyzer': 'areaCodeAnalyzer', 'searchAnalyzer': 'lucene.standard', 'type': 'string'}}}, 'name': 'namesLocAreaCd', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'person', 'database': 'search', 'indexID': '63462cc071a57f412be694d3', 'mappings': {'dynamic': False, 'fields': {'first_name': {'foldDiacritics': False, 'maxGrams': 10, 'minGrams': 2, 'tokenization': 'edgeGram', 'type': 'autocomplete'}, 'middle_name': {'foldDiacritics': False, 'maxGrams': 10, 'minGrams': 2, 'tokenization': 'edgeGram', 'type': 'autocomplete'}}}, 'name': 'personNamesAutocomplete', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'testColl', 'database': 'test', 'indexID': '629c536f31975f02afa49310', 'mappings': {'dynamic': False, 'fields': {'activity': {'minGrams': 3, 'tokenization': 'nGram', 'type': 'autocomplete'}}}, 'name': 'activities', 'status': 'STEADY', 'synonyms': []}, {'collectionName': 'mocomposed', 'database': 'test', 'indexID': '637f093498cd6435710a58d6', 'mappings': {'dynamic': True}, 'name': 'def1', 'status': 'STEADY', 'synonyms': []}]
    
    counter = 0
    print(f"Creating index")
    for index in allSrcSearchIndexes:
        indexCreateResponse = createSearchIndex(createSearchIndexURL, json.dumps(index), auth)
        
        counter += 1
        print(f"\t{counter} \t {index['database']}.{index['collectionName']} - {index['name']}")
        print(f"\t {indexCreateResponse}")
        


    # processURL = f"{projectURL}/processes"
    # rsClusterPrimaries = getAllRSClusterPrimaries(processURL, auth)

