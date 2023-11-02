# NYC Taxi Star Schema

[Introduction](#introduction) --
[Design](#designing-the-schema) --
[Infrastructure](#infrastructure-setup) -- 
[Data Engineering](#data-engineering-with-adf--databricks) -- 

### Built with:
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/databricks-%23000000.svg?style=for-the-badge&logo=databricks&logoColor=white)
![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/apache%20spark-%23FDEE21.svg?style=for-the-badge&logo=apachespark&logoColor=black)

## Introduction
This project consists of designing a Star Schema for the New York taxi dataset that involves a multi-step process. First, ingest the data into a staging area, referred to as the "bronze" layer, without imposing a predefined schema. Next, process and refine the incoming data to adhere to a specified schema, creating the "silver" layer. Finally, organize the data into a Star Schema format where the Facts and Dimensions are distinctly separated, facilitating seamless reporting through tools like Power BI.

## Designing the Schema

Created three different database designs; conceptual, logical, physical to model the structures, form, and relationships of information. The conceptual database design consisted of only the entities and relationships. The logical database design consisted of the entities, relationships, and attributes. The physical database design consisted of the entities, relationships, named attributes, keys, data types, and null values.
   <details>
   <summary>Conceptual Database Design</summary>

   ><p align="center">
   ><img src="./Designs/Conceptual database design.png"
   >  alt="Image of Conceptual Database Design"
   >  width="960" height="540">
   ></p>
   >
   Designs/Conceptual database design.png
   </details>
   
   <details>
   <summary>Logical Database Design</summary>

   ><p align="center">
   ><img src="./Designs/Logical data model.png"
   >  alt="Image of Logical Database Design"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Physical Database Design</summary>

   ><p align="center">
   ><img src="./Designs/Physical data model.png"
   >  alt="Image of Physical Database Design"
   >  width="960" height="540"
   ></p>
   >
   
   </details>

## Infrastructure Setup

Used terraform to set up the storage containers (landing, bronze, silver, gold) in the Azure storage account. A terraform script was created using VSCode and can be viewed in the infrastructure.tf file.


## Data Engineering With ADF & Databricks
Created a pipeline showing a copy data activity and 4 Databricks notebook using Azure Data Factory.

**The pipeline:**
<img src="./Images/Data Factory Pipeline.png">

**Notebook 1 (Reset)**

- Removes any files within the landing, bronze, silver and gold container.

**Copy data activity (Copy to Landing)**

- Moved data from source container to the landing container using the copy data tool in ADF. Use the LinkedService to connect ADF to the Azure Data Lake Storage Gen2 account.

**Notebook 2 (Bronze)**

- Move and transformed data files from a "landing" container to a "bronze" container in an Azure Storage account using PySpark. Read CSV files for taxi trip data from various years (2010, 2014, and 2019), added columns for the file name and processing timestamp, renameed the "VendorID" column to "vendor_id" for specific years, and partitioned the data by "vendor_id" before saving it in Delta Lake format in the "bronze" container.

**Notebook 3 (Silver)**

- Moved data from a "bronze" container to a "silver" container within an Azure Storage account using PySpark and applying data transformations. Configured access to the Azure Storage account, specified the paths for reading data from the "bronze" container for three different years (2010, 2014, and 2019), and set the destination path to the "silver" container. 

- For the years 2010 and 2014 it contained pickup and dropoff lattitude and longtidue so I created a user-defined function (map_location_udf) to map latitude and longitude coordinates to location IDs using a geospatial lookup from a GeoJSON file.

- I also applied various data transformations including data type conversions and mapping values for columns like vendor_id and payment_type, renamed columns to create standardized schemas for each of the years and finally, filtered out records where "total_amount" is non-null and greater than 0, and "trip_distance" is greater than 0 for each year. 

- I sampled the years 2010 and 2014 as the cluster was struggling to process the entire DataFrame and the filtered data is then written to the "silver" container, partitioned by "pickup_location_id." The mode("overwrite") and mode("append") options ensure that data is either replaced or appended in the "silver" container.

**Notebook 4 (Gold)**

- Moved data from a "silver" container to a "gold" container in Azure Storage using PySpark and created dimension tables and a fact table. Configured access to the Azure Storage account, loaded data from the "silver" container into the df_silver DataFrame, and then constructed dimension tables such as "dim_time", "dim_payment_type", "dim_rate_code" and "dim_vendor" by selecting and transforming specific columns. A fact table is created by selecting relevant columns, including the addition of a unique identifier column ("trip_id"). All these tables are saved in Delta Lake format in the "gold" container.
