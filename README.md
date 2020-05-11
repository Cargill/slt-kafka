# slt-kafka
Framework for bridging SAP SLT events to Kafka

## Code of Conduct

slt-kafka operates under the [Cargill Code of Conduct](https://github.com/Cargill/code-of-conduct/blob/master/code-of-conduct.md).

## Overview

The tables and ABAP code in this repository can be used to allow SLT to take the batch of records from the standard process, convert it to JSON, and send that payload to a Kafka topic. The code includes some error handling to maintain the correct order of transactional events.

## One time Setup

In order to use this process there are a few steps that need to be done one time (per environment in your landscape)

* Add a record to the TVARVC table. 

```
    NAME = ZBI_KAFKA_SERVER
    TYPE = P
    LOW =  <The URL of the kafka rest proxy>  ex.  HTTP://MY.KAFKAPROXY.COM:8082/TOPICS
```

Documentation about the Kafka REST proxy can be found here.
https://docs.confluent.io/1.0/kafka-rest/docs/intro.html 

* Ensure there are no firewalls between your SLT server an your Kafka REST proxy.

* Create all of the ABAP tables based on the definitions in associated .jpg files

* Create all the INCLUDE program ZINCLUDE_SEND_TO_KAFKA  and all of the other ABAP programs in this repo.

    * ZKAFKA_ERROR_RECORDS
    * ZKAFKA_MT_TPCPRE
    * ZKAFKA_PRINT_ERROR_RECORD
    * ZKAFKA_TBL_LTRS
    * ZKAFKA_TBL_TOPIC
    * ZTRUNCATE_KAFKA_ERROR
    * ZTRUNCATE_KAFKA_TBL_LOG

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

This program adds records to all of the standard SLT and custom Kafka tables necessary to replicate tables to Kafka. It provides a convenient way to perform this task, although it is possible to do it manually using the T-code LTRS and ZKAFKA_TBL_TOPIC. The main things this does is that it populates the ZKAFKA_TBL_TOPIC table with the default topic for that configuration. It also adds 2 columns to the table (UPDATE_TS and CHANGE_FLAG).  And it also adds the rule to call the ZINCLUDE_SEND_TO_KAFKA.

### ZKAFKA_TBL_TOPIC

This program allows you to manually update or insert the association between a configuration/table and the kafka topic you wish it to write to. You can also use this program to activate a more detailed level of logging for a particular table.

### ZKAFKA_ERROR_RECORDS

Should the include program encounter errors when calling the REST API, it will save the batch of records to the table ZKAFKA_ERROR.  This program can be used to re-process the records once you have resolved the issue.  Please note that the standard include program also checks for any error records and attempts to process them before the new batch of records. Thus maintaining the correct order of transactions.

### ZKAFKA_MT_TPCPRE

When you set up a new configuration, you need to execute this program once to set up the default topic for that configuration.

### ZKAFKA_PRINT_ERROR_RECORD

Because one of the columns in the ZKAFKA_ERROR table is of type STRING, you can not use SE16 to read the full record. You can use this program to print out the first X records in this table OR to print out a specific record if you provide the primary key. The main purpose of this is to help identify issues with the JSON formatting. The Kafka REST API may return a 500 error, but it won't provide you with any details about why it returned that. If you look at the recent records, you may be able to determine a bad numeric value or a non-unicode character.

### ZTRUNCATE_KAFKA_ERROR

This program can be used to truncate the ZKAFKA_ERROR table

### ZTRUNCATE_KAFKA_TBL_LOG

This program can be used to truncate the ZTRUNCATE_KAFKA_TBL_LOG table
