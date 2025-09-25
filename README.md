# Object Store Project
Objects Store Project is backend & frontend for working with data in AWS S3. User can view & download the data via SQL. 

## Description
User performs a query using some path. API Gareway triggers Lambda that validates the query. Lambda is responsible for returning back status codes (404, etc). 

## Action
- select path is used, Lambda performs this query on object_store table and returns back list of files and metadata. 
- download path is used, Lambda performs the query and saves the result as parquet file (presigned/request_id.parquet). Lambda creates presigned url and returns it to the user. This presigned url will be available somewhere in the future. Lambda triggers ECS Task and pass key (request_id) as environment variable. ECS Task is responsible for coping and zipping data. 

## Paths
- alive - check if service alive
- select - get information about files based on query
- download - download files based on query
- catalog - view existing files

## List of Resources
- AWS S3 - stores data & index and result of the backend operation
- AWS API Gateway - main entry for backend
- AWS Lambda - riggered by API Gateway, runs query and starts ECS for downloading
- AWS ECS - triggered by Lambda for downloading data
