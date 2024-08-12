
from utility.validation_library import write_output

from pyspark.sql.functions import (
    count,col, when,upper, isnan,lit,sha2,concat,regexp_extract)

def schema_check(source, target, spark, Out, row):

    source.createOrReplaceTempView("source")
    target.createOrReplaceTempView("target")
    source_schema = spark.sql("describe source")
    source_schema.createOrReplaceTempView("source_schema")
    target_schema = spark.sql("describe target")
    target_schema.createOrReplaceTempView("target_schema")

    failed = spark.sql('''select * from (select lower(a.col_name) source_col_name,lower(b.col_name) target_col_name, a.data_type as source_data_type, b.data_type as target_data_type, 
    case when a.data_type=b.data_type then "pass" else "fail" end status
    from source_schema a full join target_schema b on lower(a.col_name)=lower(b.col_name)) where status='fail' ''')
    source_count = source_schema.count()
    target_count = target_schema.count()
    failed_count = failed.count()
    failed.show()
    if failed_count > 0:
        write_output(validation_Type="schema_check", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column="NOT APP", Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="schema_check", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column="NOT APP", Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)


    print(source_schema.printSchema())