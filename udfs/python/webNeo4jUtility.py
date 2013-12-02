from pig_util import outputSchema

from py2neo import neo4j
from py2neo import node, rel

@outputSchema('nodeCreated:int')
def createNode(nodeValue, sLabel):
    if nodeValue:
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        alice,=graph_db.create(node(name=nodeValue))
        alice.add_labels(sLabel)
        alice["title"]="sometitle"
        return 1
    else:
        return 0

@outputSchema('nodeCreated:int')
def createRelationship(fromNode, toNode, sRelationship):
    if fromNode:
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        ref_node = graph_db.node(fromNode)
        to_node = graph_db.node(toNode)
        aliceRel=graph_db.create(rel(ref_node,sRelationship,to_node))
        return 1
    else:
        return 0   

#myudf.py
@outputSchema('nodeCreated:int')
def createBillNode(nodeValue, sLabel, sTitle, sUpdated, sBillType,sBillNumber,sIntroducedAt,sStatus,sStatusAt):
    if nodeValue:
        neo4j.authenticate("congressbills.sb01.stations.graphenedb.com:24789","congressBills", "YjaflAMKwSanXT6jsaZD")
        graph_db = neo4j.GraphDatabaseService("http://congressBills.sb01.stations.graphenedb.com:24789/db/data/")
        foundNode,=graph_db.create(node(name="steve"))
#        foundNode.add_labels(sLabel)
#        foundNode["title"]=sTitle
#         foundNode["updateDate"]=sUpdated
#         foundNode["billType"]=sBillType
#         foundNode["billNumber"]=sBillNumber
#         foundNode["introducedAt"]=sIntroducedAt
#         foundNode["status"]=sStatus
#         foundNode["statusDate"]=sStatusAt
        return 1
    else:
        return 0

#myudf.py
@outputSchema('nodeUpdated:int')
def updateBillNode(nodeID, sTitle, sUpdated, sBillType,sBillNumber,sIntroducedAt,sStatus,sStatusAt):
    if nodeValue:
        graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
        foundNode.node(nodeID)
        foundNode["title"]=sTitle
        foundNode["updateDate"]=sUpdated
        foundNode["billType"]=sBillType
        foundNode["billNumber"]=sBillNumber
        foundNode["introducedAt"]=sIntroducedAt
        foundNode["status"]=sStatus
        foundNode["statusDate"]=sStatusAt
        return 1
    else:
        return 0
