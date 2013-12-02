-- 'Document' is the delimiter
-- 'event, gathering' is the tag list
 
%default OUTPUT_PATH '/Users/davidfauth/MortarBillsData'
REGISTER '/Users/davidfauth/mortarProjects/billsProject/udfs/python/billsProject.py' USING streaming_python AS nltk_udfs;
REGISTER '/Users/davidfauth/Downloads/elasticsearch-hadoop-1.3.0.M1.jar';
define ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.resource=govtrack/bills');
 
 
bills =  LOAD '/Users/davidfauth/MortarData/' 
	USING org.apache.pig.piggybank.storage.JsonLoader(
	'bill_id:chararray, congress:chararray, official_title:chararray, updated_at:chararray, subjects_top_term:chararray,summary:map[],
	sponsor:map[], subjects');
 
billDetails = FOREACH bills 
	                 GENERATE bill_id, 
	                          congress, 
							  official_title,
							  updated_at,
							  subjects_top_term,
							  sponsor#'name' as sponsorName:chararray,
							  sponsor#'state' as sponsorState:chararray,
							  subjects AS subjectList: {t: (subjects: chararray)},
	                          summary#'text' AS billText:chararray;
	                          
billSearch = FOREACH bills
	GENERATE bill_id, 
		congress, 
		official_title,
		updated_at,
		subjects_top_term,
		sponsor#'name' as sponsorName:chararray,
		sponsor#'state' as sponsorState:chararray,
		summary#'text' AS billText:chararray;	                          
	
-- Group the tweets by place name and use a CPython UDF to find the top 5 bigrams
-- for each of these places.
bigrams_by_place = FOREACH (GROUP billDetails BY subjects_top_term) GENERATE
							                        group AS subjects_top_term:chararray, 
							                        nltk_udfs.top_5_bigrams(billDetails.official_title), 
							                        COUNT(billDetails) AS sample_size;
 
top_100_places = LIMIT (ORDER bigrams_by_place BY sample_size DESC) 100;
--STORE billSearch INTO 'govtrack/bills' USING org.elasticsearch.hadoop.pig.ESStorage();	
	
rmf $OUTPUT_PATH;
 
STORE top_100_places INTO '/Users/davidfauth/MortarBillsData' USING PigStorage('\t');