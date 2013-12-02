-- 'Document' is the delimiter
-- 'event, gathering' is the tag list
%default OUTPUT_PATH '/Users/davidfauth/MortarBillsData'
%default INPUT_PATH '/Users/davidfauth/MortarNeoTestData'

REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/billsProject.py' USING streaming_python AS nltk_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/utilities.py' USING streaming_python AS utility_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/neo4JUtility.py' USING streaming_python AS neo4j_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/webNeo4JUtility.py' USING streaming_python AS webneo4j_udfs;

rmf $OUTPUT_PATH;
bills = LOAD '$INPUT_PATH' 
USING org.apache.pig.piggybank.storage.JsonLoader(
'bill_id:chararray, congress:chararray, official_title:chararray, updated_at:chararray, subjects_top_term:chararray,summary:map[],
sponsor:map[], subjects:map[],cosponsors:map[], bill_type:chararray, number:chararray,introduced_at:chararray,status:chararray,status_at:chararray');

data = LOAD '$INPUT_PATH' 
USING org.apache.pig.piggybank.storage.JsonLoader();
out         =    FOREACH data GENERATE flatten(object#'subjects') AS subjects:chararray;
out2         =    FOREACH bills GENERATE flatten(subjects#'subjects') AS subjects:chararray;
STORE out INTO '/Users/davidfauth/MortarBillsData/subjects' USING PigStorage('\t');
subjectsList = LOAD '/Users/davidfauth/MortarBillsData/subjects' USING PigStorage('\t') as (keyValue:chararray);
uniqueSubjList = ORDER subjectsList by keyValue ASC;
bagList = FOREACH bills GENERATE bill_id, official_title, updated_at, bill_type, number, introduced_at, status, status_at;
billnodeValue = FOREACH bagList GENERATE webneo4j_udfs.createBillNode(bill_id, 'testBill',official_title,updated_at, bill_type, number, introduced_at, status, status_at) as nodeCreated;

--nodeValue = FOREACH bills GENERATE neo4j_udfs.createNode(bill_id, 'testBill') as nodeCreated;
--STORE nodeValue INTO '$OUTPUT_PATH/logs/bills' USING PigStorage('\t');
STORE billnodeValue INTO '$OUTPUT_PATH/logs/billNodeVlaue' USING PigStorage('\t');