# slt-kafka
Framework for bridging SAP SLT events to Kafka

## Code of Conduct

slt-kafka operates under the [Cargill Code of Conduct](https://github.com/Cargill/code-of-conduct/blob/master/code-of-conduct.md).

## Overview

The tables and ABAP code in this repository can be used to allow SLT to take the batch of records from the standard process, convert it to JSON, and send that payload to a Kafka topic. The code includes some error handling to maintain the correct order of transactional events.

## One time Setup

In order to use this process there are a few steps that need to be done one time (per environment in your landscape)

* Add a record to the TVARVC table. 
NAME = ZBI_KAFKA_SERVER
TYPE = P
LOW =  <The URL of the kafka rest proxy>  ex.  HTTP://MY.KAFKAPROXY.COM:8082/TOPICS

Documentation about the Kafka REST proxy can be found here.
https://docs.confluent.io/1.0/kafka-rest/docs/intro.html 

* Ensure there are no firewalls between your SLT server an your Kafka REST proxy.

* Create all of the ABAP tables based on the definitions in associated .jpg files

* Create all the INCLUDE program ZINCLUDE_SEND_TO_KAFKA  and all of the other ABAP programs in this repo.
ZKAFKA_ERROR_RECORDS
ZKAFKA_MT_TPCPRE
ZKAFKA_PRINT_ERROR_RECORD
ZKAFKA_TBL_LTRS
ZKAFKA_TBL_TOPIC
ZTRUNCATE_KAFKA_ERROR
ZTRUNCATE_KAFKA_LOGGING
ZTRUNCATE_KAFKA_TBL_LOG

## Once per SLT Configuration Setup

The below steps need to be performed any time you create a new SLT configuration.

* Execute the program ZKAFKA_MT_TPCPRE with the default topic for that configuration.  

## Once per Table Configuration Setup

The below steps can be executed with 1 to many tables at 1 time

* Execute the program ZKAFKA_TBL_LTRS passing it the Congfiguration ID and the list of tables.  This will create the required records in the LTRS tables for these tables and also set the default topic for those tables.

* Optional Step - If you would like to override the default topic for that table you can execute the program ZKAFKA_TBL_TOPIC

* Ensure that the Kafka topic exists or auto creation is turned on.

* Start Replication of the table following the standard processes.


## Program Explanations

### ZINCLUDE_SEND_TO_KAFKA

This is the main ABAP code that executes on every SLT batch/loop. At a high level it takes the internal table that the data is stored in,  converts it to JSON, and finally calls the Kafka REST API.

### ZKAFKA_TBL_LTRS



### ZKAFKA_TBL_TOPIC

### ZKAFKA_ERROR_RECORDS

Should the include program encounter errors when calling the REST API, it will save the batch of records to the table ZKAFKA_ERROR.  This program can be used to re-process the records once you have resolved the issue.  Please note that the standard include program also checks for any error records and attempts to process them before the new batch of records. Thus maintaining the correct order of transactions.

### ZKAFKA_MT_TPCPRE
### ZKAFKA_PRINT_ERROR_RECORD

### ZTRUNCATE_KAFKA_ERROR
### ZTRUNCATE_KAFKA_LOGGING
### ZTRUNCATE_KAFKA_TBL_LOG
