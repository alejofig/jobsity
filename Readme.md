Main script is developed to run in AWS glue.
It takes a file in a s3 bucket, process info and put the result in a database.
It is mandatory to fill the env file (with the database info).

It has a file called aws_diagram that explains the architecture in aws and a data model with the relational
data of the process.

The queries file has the queries to compute some analysis of the data.
