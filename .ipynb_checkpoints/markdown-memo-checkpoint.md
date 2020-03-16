# hello

## this is second line

#demo 2

Analyzing New York City taxi data using big data tools

https://developers.arcgis.com/python/sample-notebooks/analyze-new-york-city-taxi-data/

Analyze the NYC Taxi Data

https://chih-ling-hsu.github.io/2018/05/14/NYC



## nyc data source

https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Taxi Zone Maps and Lookup Tables
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip


### resource

https://www1.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf


## azure open data

NYC Taxi & Limousine Commission - green taxi trip records

https://opendata.cityofnewyork.us/

https://azure.microsoft.com/zh-cn/services/open-datasets/catalog/nyc-taxi-limousine-commission-green-taxi-trip-records/

https://azure.microsoft.com/en-us/services/open-datasets/catalog/nyc-taxi-limousine-commission-yellow-taxi-trip-records/

https://azure.microsoft.com/zh-cn/services/open-datasets/catalog/nyc-taxi-limousine-commission-yellow-taxi-trip-records/#AzureNotebooks


## Big Data Features

https://bigdataknowhow.com/aws-big-data-visualization/


## Tools to use

https://amazonaws-china.com/cn/blogs/big-data/10-visualizations-to-try-in-amazon-quicksight-with-sample-data/

Visualize AWS Cloudtrail Logs Using AWS Glue and Amazon QuickSight

https://amazonaws-china.com/cn/blogs/big-data/streamline-aws-cloudtrail-log-visualization-using-aws-glue-and-amazon-quicksight/

https://docs.aws.amazon.com/zh_cn/quicksight/latest/user/create-a-data-set-athena.html


https://amazonaws-china.com/cn/blogs/big-data/amazon-quicksight-adds-support-for-combo-charts-and-row-level-security/


## amazon Redshift

https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-clean-up-tasks.html


## amazon emr notebook quicksight

https://docs.aws.amazon.com/zh_cn/quicksight/latest/user/supported-manifest-file-format.html
https://docs.aws.amazon.com/zh_cn/quicksight/latest/user/create-a-data-set-athena.html
https://docs.aws.amazon.com/zh_cn/emr/latest/ReleaseGuide/emr-spark-glue.html
https://docs.aws.amazon.com/zh_cn/emr/latest/ReleaseGuide/emr-spark-configure.html


## pyspark 

https://github.com/pogzyb/pyspark-aws-example/tree/master/notebook

https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.html?highlight=plot#pandas.DataFrame.plot

## Amazon Redshift load data from s3

https://docs.aws.amazon.com/zh_cn/redshift/latest/gsg/rs-gsg-create-sample-db.html




# resource 

s3://data-etl-o-original-raw/green/tlc.green_tripdata_2019-01.csv

s3://data-etl-o-target-0/result/green

s3://data-etl-o-target-0/logs

s3://temp-data-dir

s3://data-etl-o-original-raw/zone/nyc.tlc.taxi_zone_lookup.csv
s3://data-etl-o-original-raw/zone/nyc.tlc.taxi_zone_lookup.csv

arn:aws:s3:::aws-logs-622309853439-ap-northeast-1

s3://aws-logs-622309853439-ap-northeast-1

arn:aws:s3:::aws-emr-resources-622309853439-ap-northeast-1

s3://aws-emr-resources-622309853439-ap-northeast-1

s3://data-etl-1-s3-demo-1/raw/nyc.tlc.taxi_zone_lookup.csv

s3://data-etl-o-original-raw/zone/nyc.tlc.taxi_zone_lookup.csv
s3://data-etl-o-original-raw/zone/taxi_zones_shape.zip


## target user imporant role 

 1) business analyst for trends; 
 2) operation analyst to decide the dynamic pricing; 
 3) drivers to understand the predicted demands in near future

### required to design a PoC of this solution for driving followon business.

+ Retrieve data from raw format
+ Perform data analysis (feature engineering) for data ETL
+ Illustrate request and demands trends, especially for the gap trends of taxi for data analyst for insight discovery
+ Provide graphic illustration for operators to understand the changes of demands gap in different area (location ID). If possible, you could make a short 10-min prediction
+ Let drivers to see the demands level in 10 min drive distance. If possible, display the past, current and future in graphics
+ Setup a portal for different users to login to watch different GUI


## what should i do ?

+ build an map-reduce task to load the source data from S3,
+ perform ETL to clean the data and store them back to S3; 
+ generate Data Visualization result with any tools you are familiar with. 
+ You need to provide a document to explain your design and a workable applications on AWS for demonstration.
+ Setup a portal for different users to login to watch different GUI

demonstrate the result of your design. 

You need to at least give some statics on: 

+ travel distance
+ travel time
+ average speed
+ hot spots in Manhattan (if any), 
+ trends analysis for taxi loads.


For business analytes and operators, you need to give a common trends analysis of such data and any relevant data you think is important to illustrate. And, 

you need to give the user some input boxes for them to define the search criteria, such as latitude/longitude boxes, time ranges, long distance (>15 miles) and etc.


#### Recommend a manageable, secure, scalable, high performance, efficient, elastic, highly available, fault tolerant and recoverable architecture that allows ABC to grow with AWS.
+ Tell the audience how to build a business solution quickly and reliably on AWS.


### code

jupyter labextension install @krassowski/jupyterlab_go_to_definition
https://github.com/krassowski/jupyterlab-lsp

https://amazonaws-china.com/cn/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/

https://github.com/aws-samples/amazon-eks-apache-spark-etl-sample/blob/master/src/main/scala/ValueZones.scala


#### glue
https://github.com/aws-samples/aws-glue-samples/blob/master/examples/resolve_choice.py
https://github.com/aws-samples/aws-glue-samples/blob/master/examples/data_cleaning_and_lambda.py
https://github.com/aws-samples/amazon-eks-apache-spark-etl-sample/blob/master/src/main/scala/ValueZones.scala

%%configure -f
{ "conf":{
          "spark.pyspark.python": "python",
          "spark.pyspark.virtualenv.enabled": "true",
          "spark.pyspark.virtualenv.type":"native",
          "spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"
         }
}

https://docs.aws.amazon.com/zh_cn/glue/latest/dg/aws-glue-programming-python.html
https://amazonaws-china.com/cn/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/
https://docs.aws.amazon.com/zh_cn/glue/latest/dg/edit-script.html
https://docs.aws.amazon.com/zh_cn/glue/latest/dg/aws-glue-programming-python-samples-legislators.html
https://github.com/aws-samples/amazon-eks-apache-spark-etl-sample/blob/master/src/main/scala/ValueZones.scala

s3://data-etl-o-target-0/result/part-00000-8966722a-b52d-424c-9e60-a97f87596833-c000.snappy.parquet