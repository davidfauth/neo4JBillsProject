-- 'Document' is the delimiter
-- 'event, gathering' is the tag list

%default OUTPUT_PATH '/Users/davidfauth/MortarBillsData'
%default S3_OUTPUT_PATH 's3n://df-bills-project'
%default S3_INPUT_PATH 's3n://df-bills-data'
%default INPUT_PATH '/Users/davidfauth/MortarNeoTestData'
%default BULK_INPUT_PATH '/Users/davidfauth/MortarTestDataBulk'
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/billsProject.py' USING streaming_python AS nltk_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/utilities.py' USING streaming_python AS utility_udfs;
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/neo4JUtility.py' USING streaming_python AS neo4j_udfs;

rmf $OUTPUT_PATH;
--rmf $S3_OUTPUT_PATH;

bills = LOAD '$BULK_INPUT_PATH' 
USING org.apache.pig.piggybank.storage.JsonLoader(
'bill_id:chararray, congress:chararray, official_title:chararray, updated_at:chararray, subjects_top_term:chararray,summary:map[],
sponsor:map[], subjects:map[],cosponsors:map[], bill_type:chararray, number:chararray,introduced_at:chararray,status:chararray,status_at:chararray');


data = LOAD '$BULK_INPUT_PATH' 
USING org.apache.pig.piggybank.storage.JsonLoader();


-- get unique list of bills, subjects, sponsors and cosponsors to create nodes
billList = FOREACH bills GENERATE bill_id as keyValue, 'bill' as nodeType;
congressList = FOREACH bills GENERATE congress as keyValue, 'congress' as nodeType;
congressBillList = FOREACH bills GENERATE congress as congressID, bill_id; 

sponsors = FOREACH bills GENERATE bill_id,
sponsor#'name' AS sponsorName:chararray,
sponsor#'state' AS sponsorState:chararray,
sponsor#'district' AS sponsorDistrict:chararray, 
CONCAT(CONCAT(sponsor#'name',' '),sponsor#'state') as keyValue:chararray;

--sponsorNameKey = FOREACH sponsors GENERATE CONCAT(CONCAT(sponsorName,' '),sponsorState) as keyValue:chararray, 'MemberOfCongress' as nodeType;
listSponsors = FOREACH sponsors GENERATE keyValue;
sponsorNameKey = FOREACH sponsors GENERATE keyValue, 'sponsor' as nodeType;

cs = FOREACH data GENERATE flatten(object#'bill_id') as billid,flatten(object#'cosponsors') AS cosponsors:map[];
names = FOREACH cs GENERATE billid as bill_id, flatten(cosponsors#'name') as coSponsorName:chararray, 
flatten(cosponsors#'state') as coSponsorState:chararray,
flatten(cosponsors#'district') as coSponsorDistrict:chararray,
CONCAT(CONCAT(cosponsors#'name',' '),cosponsors#'state') as keyValue:chararray;

cosponsorNameKey = FOREACH names GENERATE CONCAT(CONCAT(coSponsorName,' '),coSponsorState) as keyValue:chararray, 'MemberOfCongress' as nodeType;
listCoSponsors = FOREACH names GENERATE keyValue;

-- create list of distinct sponsors/cosponsors
unionSponsorCoSponsors = UNION listSponsors, listCoSponsors;
bUnion = GROUP unionSponsorCoSponsors BY 1;
cUsCS = FOREACH bUnion GENERATE flatten(unionSponsorCoSponsors);
listdistinctSponsorsCosponsors = DISTINCT cUsCS;

uniqueCongressList = DISTINCT congressList;
uniquebillList = DISTINCT billList;
uniqueSponsors = DISTINCT sponsorNameKey;
uniqueCoSponsors = DISTINCT cosponsorNameKey;


-- create the subject List
-- for some reason, it needs to be written out to file and brought back in

subjectList = FOREACH data GENERATE object#'bill_id' as bill_id:chararray, flatten(object#'subjects') AS keyValue:chararray;
STORE subjectList INTO '/Users/davidfauth/MortarBillsData/subjects' USING PigStorage('\t');
subjectData = LOAD '/Users/davidfauth/MortarBillsData/subjects' USING PigStorage('\t') as (bill_id:chararray, keyValue:chararray);
tmpSubjectList = FOREACH subjectData GENERATE keyValue;
uniqueSubjectList = DISTINCT tmpSubjectList;

keySubjectList = FOREACH uniqueSubjectList GENERATE keyValue, 'subject' as nodeType;

ordereduniqueSubjList = ORDER keySubjectList by keyValue ASC;
ordereduniqueBillListValues = ORDER uniquebillList BY keyValue;
orderedUniqueSponsors = ORDER uniqueSponsors BY keyValue;
orderedUniqueCoSponsors = ORDER uniqueCoSponsors BY keyValue;
orderedUniqueSCoS = ORDER listdistinctSponsorsCosponsors By keyValue;


-- create the key values (list of nodes) that Neo4J will use
unionKeys = UNION uniqueCongressList, ordereduniqueSubjList, ordereduniqueBillListValues, orderedUniqueSponsors, orderedUniqueCoSponsors;
--unionKeys = UNION uniqueCongressList, ordereduniqueSubjList, ordereduniqueBillListValues, orderedUniqueSCoS;
b = GROUP unionKeys BY 1;
c = FOREACH b GENERATE flatten(unionKeys);

-- run the Counter UDF to create a Node ID
keyNodeList = FOREACH c GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int, nodeType;

-- Log nodeCreation
STORE keyNodeList INTO '$OUTPUT_PATH/logs/keyNodeList' USING PigStorage('\t');
STORE c INTO '$OUTPUT_PATH/logs/unionValues' USING PigStorage('\t');
