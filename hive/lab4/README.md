Main findings:  
— sometimes Hive fails to create index in place and asks to use DEFERRED REBUILD with following explicit REBUILD command, not very convenient  
— indexing doesn't play well with ORC and PARQUET file formats (i.e. index rebuild fails with java.lang.OutOfMemoryError exception, likely to be environment-specific, though)  
— indexing most of the has no observable effect on query execution time, index _should_ be used explicitly (not included in this report, but few ad hoc experiments support that)  
— a query to a partitioned table in PARQUET format will fail with "java.lang.IllegalArgumentException: Column [%columnName of the column table was partitioned by%] was not found in schema!" message if there's a UNION operator in it  
— vectorization have no significant effect either, maybe data/schema/queries should be adjusted for it to kick in  
— TEZ has a little impact on execution speed when used with a file in TEXTFILE format, but shows tremendous performance gains with AVRO (up to 2 times), RCFILE/ORC (up to 20 times faster) and PARQUET (2-5 times faster)  
— partitioning has a noticeable effect on execution speed of queries that rely on column that table was partitioned by (1.5-2 times faster)  
— clustering has a clear negative impact on queries' performance, maybe schema or queries should be tuned for it to give positive effect  
— file format has a great effect on table size (from 670 MB in TEXTFILE to 27 MB in ORC) and query execution speed (due to "built-in indexes" I guess)  
