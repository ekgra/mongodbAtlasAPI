import requests
from requests.auth import HTTPDigestAuth
import pprint
from atlasConfig import atlasConf as conf

def getAllReplicaSetsInCluster(processURL, auth): 
    response = requests.get(processURL, auth = auth)
    replicaSetsInCluster = set()
    for d in response.json()['results']:
        replicaSetsInCluster.add(d['replicaSetName']) 
        return replicaSetsInCluster

if __name__ == "__main__":
    
    auth = HTTPDigestAuth(conf["ATLAS_PUBLIC_KEY"], conf["ATLAS_PRIVATE_KEY"])

    projectURL = f'https://cloud.mongodb.com/api/atlas/v1.0/groups/{conf["PROJECT_ID"]}'
    clusterURL = f'{projectURL}/clusters/conf["CLUSTER_NAME"]'

    processURL = f"{projectURL}/processes"
    allSearchIndexesForCollectionURL = f"{processURL}/fts/indexes/sample_mflix/nosearch"

    replicaSetsInCluster = getAllReplicaSetsInCluster(processURL, auth)
    print(replicaSetsInCluster)