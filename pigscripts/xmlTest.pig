-- 'Document' is the delimiter
-- 'event, gathering' is the tag list

data = LOAD '/Users/davidfauth/bills/113/s/s3/data.xml'
       USING org.apache.pig.piggybank.storage.StreamingXMLLoader(
          'bill',
          'cosponsors',
          'summary'
       ) AS (
           cosponsors: {(attr:map[], content:chararray)},
           summary: {(attr:map[], content:chararray)}
       );

dump data;
