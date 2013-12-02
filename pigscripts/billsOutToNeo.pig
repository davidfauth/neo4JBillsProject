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

billNodes = LOAD '$OUTPUT_PATH/logs/keyNodeList' USING PigStorage('\t') 
        AS (keyValue:chararray, nodeID:int, nodeType:chararray);

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

-- run the counter UDF inside the single reducer
--numBillKeyValue = FOREACH ordereduniqueBillListValues GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int;
--numSponsorsKeyValue = FOREACH orderedUniqueSponsors GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int;
--numCoSponsorsKeyValue = FOREACH orderedUniqueCoSponsors GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int;
--numSubjectsKeyValue = FOREACH ordereduniqueSubjList GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int;

-- run the Counter UDF to create a Node ID
keyNodeList = FOREACH c GENERATE keyValue, utility_udfs.auto_increment_id() AS my_id:int, nodeType;


--Create Nodes (can I group and create a tuple/values)
nodeValue = FOREACH keyNodeList GENERATE neo4j_udfs.createNode(keyValue, nodeType) as nodeCreated;
--nodeSponsorValue = FOREACH numSponsorsKeyValue GENERATE neo4j_udfs.createNode(keyValue, 'sponsor') as nodeCreated;
--nodeCoSponsorValue = FOREACH numCoSponsorsKeyValue GENERATE neo4j_udfs.createNode(keyValue, 'cosponsor') as nodeCreated;
--nodeSubjectValue = FOREACH numSubjectsKeyValue GENERATE neo4j_udfs.createNode(keyValue, 'subject') as nodeCreated;

-- Update Bill nodes with additional details
updatedBillNodes = JOIN keyNodeList by keyValue, bills by bill_id;

-- Create bills to subjects relationships
billRelBillID = JOIN keyNodeList BY keyValue, subjectData by bill_id;
billRel = JOIN billRelBillID by subjectData::keyValue, keyNodeList BY keyValue;
--relValue = JOIN billRel by keyValue, subjectData by bill_id;

--Create bills to sponsors relationships
billRelSponsorID = JOIN keyNodeList BY keyValue, sponsors by bill_id;
billSponsorRel = JOIN billRelSponsorID by sponsors::keyValue, keyNodeList BY keyValue;

--Create bills to cosponsors relationships
billCoRelSponsorID = JOIN keyNodeList BY keyValue, names by bill_id;
billCoSponsorRel = JOIN billCoRelSponsorID by names::keyValue, keyNodeList BY keyValue;

--Create Congress to Sponsor relationships
congressRelSponsorID = JOIN keyNodeList BY keyValue, congressBillList by congressID;
congressSponsorRel = JOIN congressRelSponsorID by congressBillList::bill_id, sponsors by bill_id;
congressSponsorNodes = JOIN congressSponsorRel by sponsors::keyValue, keyNodeList BY keyValue;

--Create Congress to CoSponsor relationships
congressRelCoSponsorID = JOIN keyNodeList BY keyValue, congressBillList by congressID;
congressCoSponsorRel = JOIN congressRelCoSponsorID by congressBillList::bill_id, names by bill_id;
congressCoSponsorNodes = JOIN congressCoSponsorRel by names::keyValue, keyNodeList BY keyValue;

--Create Relationships
relBillValue = FOREACH billRel GENERATE neo4j_udfs.createRelationship(billRelBillID::keyNodeList::my_id,keyNodeList::my_id,'subject_of');
relBillSponsor = FOREACH billSponsorRel GENERATE neo4j_udfs.createRelationship(keyNodeList::my_id,billRelSponsorID::keyNodeList::my_id,'sponsor_of');
relBillCoSponsor = FOREACH billCoSponsorRel GENERATE neo4j_udfs.createRelationship(keyNodeList::my_id,billCoRelSponsorID::keyNodeList::my_id,'cosponsor_of');
relCongressSponsor = FOREACH congressSponsorNodes GENERATE neo4j_udfs.createRelationship(keyNodeList::my_id,congressSponsorRel::congressRelSponsorID::keyNodeList::my_id,'member_of');
relCongressCoSponsor = FOREACH congressCoSponsorNodes GENERATE neo4j_udfs.createRelationship(keyNodeList::my_id,congressCoSponsorRel::congressRelCoSponsorID::keyNodeList::my_id,'member_of');
nodeBillDetails = FOREACH updatedBillNodes GENERATE neo4j_udfs.updateBillNode(keyNodeList::my_id, bills::official_title,bills::updated_at,bills::bill_type, bills::number, bills::introduced_at, bills::status, bills::status_at);


-- Log nodeCreation
STORE nodeBillDetails INTO '$OUTPUT_PATH/logs/nodeBillDetails' USING PigStorage('\t');
STORE nodeValue INTO '$OUTPUT_PATH/logs/bills' USING PigStorage('\t');
STORE billRel INTO '$OUTPUT_PATH/logs/billsRel' USING PigStorage('\t');
STORE keyNodeList INTO '$OUTPUT_PATH/logs/keyNodeList' USING PigStorage('\t');
STORE billRelBillID INTO '$OUTPUT_PATH/logs/billRelID' USING PigStorage('\t');
STORE relBillValue INTO '$OUTPUT_PATH/logs/billRelValues' USING PigStorage('\t');
STORE relBillSponsor INTO '$OUTPUT_PATH/logs/billRelSponsorValues' USING PigStorage('\t');
STORE relBillCoSponsor INTO '$OUTPUT_PATH/logs/billRelCoSponsorValues' USING PigStorage('\t');
STORE relCongressSponsor INTO '$OUTPUT_PATH/logs/relCongressSponsor' USING PigStorage('\t');
STORE relCongressCoSponsor INTO '$OUTPUT_PATH/logs/relCongressCoSponsor' USING PigStorage('\t');
STORE congressSponsorNodes INTO '$OUTPUT_PATH/logs/congressSponsorlRel' USING PigStorage('\t');
STORE updatedBillNodes INTO '$OUTPUT_PATH/logs/nodeBillUpdateDetails' USING PigStorage('\t');
STORE c INTO '$OUTPUT_PATH/logs/unionValues' USING PigStorage('\t');
