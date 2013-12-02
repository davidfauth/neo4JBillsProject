-- 'Document' is the delimiter
-- 'event, gathering' is the tag list

%default OUTPUT_PATH '/Users/davidfauth/MortarBillsData'
%default TEST_OUTPUT_PATH '/Users/davidfauth/TestMortarBillsData'
%default S3_OUTPUT_PATH 's3n://df-bills-project'
%default S3_INPUT_PATH 's3n://df-bills-data'
%default INPUT_PATH '/Users/davidfauth/MortarNeoTestData'
%default BULK_INPUT_PATH '/Users/davidfauth/MortarTestDataBulk'
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/billsProject.py' USING streaming_python AS nltk_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/utilities.py' USING streaming_python AS utility_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/neo4JUtility.py' USING streaming_python AS neo4j_udfs;

rmf $TEST_OUTPUT_PATH;
--rmf $S3_OUTPUT_PATH;

bills = LOAD '$BULK_INPUT_PATH' 
USING org.apache.pig.piggybank.storage.JsonLoader(
'bill_id:chararray, congress:chararray, official_title:chararray, updated_at:chararray, subjects_top_term:chararray,summary:map[],
sponsor:map[], subjects:map[],cosponsors:map[], bill_type:chararray, number:chararray,introduced_at:chararray,status:chararray,status_at:chararray');


keyNodeList = LOAD '$OUTPUT_PATH/logs/keyNodeList' USING PigStorage('\t') 
        AS (keyValue:chararray, nodeID:int, nodeType:chararray);

--Create Nodes (can I group and create a tuple/values)
nodeValue = FOREACH keyNodeList GENERATE neo4j_udfs.createNode(keyValue, nodeType) as nodeCreated;

-- Update Bill nodes with additional details
updatedBillNodes = JOIN keyNodeList by keyValue, bills by bill_id;

--Create Relationships
nodeBillDetails = FOREACH updatedBillNodes GENERATE neo4j_udfs.updateBillNode(keyNodeList::nodeID, bills::official_title,bills::updated_at,bills::bill_type, bills::number, bills::introduced_at, bills::status, bills::status_at);

-- Log nodeCreation
STORE nodeValue INTO '$TEST_OUTPUT_PATH/logs/bills' USING PigStorage('\t');
STORE nodeBillDetails INTO '$TEST_OUTPUT_PATH/logs/nodeBillUpdateDetails' USING PigStorage('\t');
