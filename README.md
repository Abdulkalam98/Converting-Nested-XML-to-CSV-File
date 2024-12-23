# AHQ Pyspark template

## variables to be specified in .gitlab-ci.yml when using this template.  

```
variables:
    s3PysparkScriptBucket:       "TO_BE_SPECIFIED"  ## S3 script bucket name in Dev env  ex: "s3://lfg-151324446874-use1-gluescripts-bucket/pyspark/dev/"  
    s3PysparkLibBucket:          "TO_BE_SPECIFIED"  ## S3 lib bucket name in Dev env     ex: "s3://lfg-151324446874-use1-gluelibrary-bucket/pyspark/dev/"  
    s3PysparkConfigBucket:       "TO_BE_SPECIFIED"  ## S3 config bucket name in Dev env  ex: "s3://lfg-151324446874-use1-glueconfig-bucket/pyspark/dev/"  
    s3PysparkInputBucket:        "TO_BE_SPECIFIED"  ## S3 config bucket name in Dev env  ex: "s3://lfg-151324446874-use1-input-bucket/pyspark/dev/"  
  
    s3PysparkScriptBucket_qa:    "TO_BE_SPECIFIED"  ## S3 script bucket name in Test env  
    s3PysparkLibBucket_qa:       "TO_BE_SPECIFIED"  ## S3 lib bucket name in Test env  
    s3PysparkConfigBucket_qa:    "TO_BE_SPECIFIED"  ## S3 config bucket name in Test env  
  
    s3PysparkScriptBucket_prod:  "TO_BE_SPECIFIED"  ## S3 script bucket name in Prod env  
    s3PysparkLibBucket_prod:     "TO_BE_SPECIFIED"  ## S3 lib bucket name in Prod env  
    s3PysparkConfigBucket_prod:  "TO_BE_SPECIFIED"  ## S3 config bucket name in Prod env  

```

## Custom environment specific variables to be specified in variables.yml when using this template.  

```
  STACKNAME               ## name should start with lfg-glue-spark-etl-<NAME>-<env>  ex: "fund_performance_bob_dev"  
  GlueServiceRole:        ## Glue Service Role   ex: "lfg-glue-service-role"  
  PythonVersion:          ## Python Version 3 recommended    ex: "3"  
  project:                ## Project name     ex: "AHQ-glue"    
  team:                   ## do not change    ex: "cloudops"    
  ScriptLocation:         ## Complete path of Python  Script ex: "s3://lfg-151324446874-glue-library-ue1-bucket001/scriptexample1.py"  
  extrapyfiles:           ## extra PY files, comma separated if more than 1   ex:  "s3://lfg-151324446874-glue-library-ue1-bucket001/example1.whl"   
  destinationbucket:      ## Destination Bucket for output *Required  ex:  "s3://lfg-151324446874-glue-temp-ue1-bucket001/"  
  environment:            ## dev, test, prod    ex:  "dev"   
  gluedbcredsecret:       ## should pre-exist, check with WH    
  GlueVersion:            ## do no change       ex: "2.0"     
  costcenter:             ## DSU /CostCenter    ex: "C01322"     
  MaxConcurrentRuns:      ## Refer documentation, this is default    ex:  "1"    
  MaxRetries:             ## Refer documentation, this is default    ex:  "0"    
  sparkeventlogspath:     ## Need to specify Logs path on S3 bucket  ex:  "s3://lfg-151324446874-glue-temp-ue1-bucket001/log"    
  WorkerType:             ## Refer to doc. Number of workers and WorkerType have dependency  ex:  "G.1X"    
  NumberOfWorkers:        ## Refer to doc. Number of workers and WorkerType have dependency  ex:  "2"   


```
