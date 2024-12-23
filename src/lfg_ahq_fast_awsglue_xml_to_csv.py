import sys
import io
import os
import re
from os import path
from copy import deepcopy
import csv
import json
import boto3
import pandas as pd
import xmltodict
import datetime
from gen_utils import extract_datetime
# from logging import RootLogger
from awsglue.utils import getResolvedOptions
from connect import *
from bs4 import BeautifulSoup

from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import *
from pyspark.sql.functions import *
import logging

logging.basicConfig(level = logging.INFO, format='%(levelname)s:%(message)s')

#Getting the job Level Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','region','csvfilename','inputbucketname','inputsubdir','inputfilepattern','confbucketname','confsubdir','conffilename','rawbucketname','rawsubdir','paramfilename','odsschemaname','orajdbcdrivername','odscatalogtablename','odscatalogdatabase','odssecretname','xmlsubdir'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#Enable Apache Arrow
spark.conf.set("spark.sql.execution.arrow.enabled","true")


#Create a temp dir in glue instance
temp_dir="/tmp/glue/"
os.makedirs(temp_dir)
os.chdir(temp_dir)
print(os.getcwd())

region_name=args['region']
inputfilepattern=args['inputfilepattern']
csv_file_name=args['csvfilename']
input_bucket_name=args['inputbucketname']
input_sub_dir=args['inputsubdir']
param_file_name=args['paramfilename']
confbucketname=args['confbucketname']
confsubdir=args['confsubdir']
conffilename=args['conffilename']
rawbucketname=args['rawbucketname']
rawsubdir=args['rawsubdir']
xmlsubdir=args['xmlsubdir']
ods_schema_name=args['odsschemaname']
jdbc_driver_name=args['orajdbcdrivername']
oracle_secret=args['odssecretname']

def extract_datetime(val):
    if val[:4]=='1900':
        return '00000000'
    elif len(val)==27:
        val=val[:-1]

    formats = [
        "%Y%m%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]

    for strptime_format in formats:
        try:
            temp= datetime.datetime.strptime(val, strptime_format)
            return datetime.datetime.strftime(temp,'%Y%m%d')
        except ValueError:
            pass

    raise ValueError("Unable to parse date {!r}".format(val))

def indicator(x):
    try:
        if extract_datetime(x) == '00000000' :
            return 'N'
        elif extract_datetime(x):
            return 'R'
    except:
        return 'N'

def errorHandle(tag_path,default=''):
    try:
        a = eval(tag_path)
        return a
    except Exception as e:
        logging.warning(f"{e}",exc_info=True)
        return f"{default}"
        

def trx_typ(trx):
    temp_cd = data_dict['CommissionExtract']['CommissionableEvent'][trx]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['@code']
    temp_desc = data_dict['CommissionExtract']['CommissionableEvent'][trx]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['#text']
    if  temp_cd == 'PP':
        pay_typ = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrCashDtl']['CashDtl']['CashDtlData']['CashDtlData']['PaymentType']['#text']")
        qualified_id = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Qualified']['#text']")
        if pay_typ.strip() == "New Money" and qualified_id.strip() == "True":
            TRANS_CD = 'PP - TRUE - NEW MONEY'
            TRANS_CD_DESC = 'Premium - True - New Money'
            return TRANS_CD,TRANS_CD_DESC
        else:
            return temp_cd,temp_desc
    elif temp_cd == "SYWD":
        requestType = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrDisburse']['TrxHdrDisburseData']['TrxHdrDisburseData']['RequestType']['#text']") 
        print(requestType)
        if requestType =="RMD":
            TRANS_CD = 'SYWD-RDM'
            TRANS_CD_DESC = 'SYWD-RDM'
            return TRANS_CD,TRANS_CD_DESC
        else:
            return temp_cd,temp_desc
    else:
        return temp_cd,temp_desc

def s3_download(download_file,s3_bucket,region_name,obj):
    #download config file from s3
    try:
        s3 = boto3.client('s3', region_name=region_name)
        ingest_key=obj+download_file
        s3.download_file(s3_bucket,ingest_key,download_file)
        print("Download Completed")
        return True
    except Exception as e:
        print("Downloading Failed")
        print(str(e))
        return False

def s3_upload(filename,s3_bucket,region_name,obj):
    try:
        ingest_key=obj+filename
        s3 = boto3.client('s3',region_name)
        s3.upload_file(filename,s3_bucket,ingest_key)
        print("file uploading complete for "+ str(filename))
    except Exception as e:
        print("uploading  failed for "+str(filename))
        print(e)

config_var=get_config(conffilename,confbucketname,region_name,confsubdir)
print("Config var",config_var)

#param file
param_var=get_config(param_file_name,confbucketname,region_name,confsubdir)
print(param_var)

##"oracle.jdbc.OracleDriver"
oracle_key=get_secret(oracle_secret,region_name)
oracle_username=oracle_key['username']
oracle_password=oracle_key['password']
oracle_host=oracle_key['host']
oracle_port=oracle_key['port']
oracle_dbname=oracle_key['dbname']
oracle_url=str("jdbc:oracle:thin:@")+str(oracle_host)+":"+str(oracle_port)+"/"+str(oracle_dbname)
print(oracle_url)

batch_date=param_var['batch_date']
src_sys=param_var['src_sys']
csv_file_name=csv_file_name+'MD'+'_'+str(batch_date)+'.txt'

filter_query = config_var['filter_query']
filter_query = filter_query.format(ods_schema_name,src_sys)
print("filter_query",filter_query)

valid_tags = get_vw(glueContext,filter_query,jdbc_driver_name,oracle_url,oracle_username,oracle_password)

valid_tags = list(valid_tags.toPandas()['FIN_TRANS_CD'])

print("valid tags extracted",valid_tags)

try:
    xml_file_name='CommissionEventExtract_202301240202705.txt'
    
    # xml_file_name = "CommissionEventExtract_20221118043802.txt" # remove later
    s3=boto3.client('s3')
    # for key in s3.list_objects(Bucket=rawbucketname,Prefix=xmlsubdir)['Contents']:
    #     xml_file_name=key['Key'].split("/")[-1]
    #     inputfilepattern = inputfilepattern.format(batch_date)
    #     pattern = re.compile(inputfilepattern)
    #     if pattern.match(xml_file_name):
    #         s3_download(xml_file_name,rawbucketname,region_name,xmlsubdir)
    #         break
    
    s3_download(xml_file_name,rawbucketname,region_name,xmlsubdir)

    #Parsing the XML file in dict
    with open(xml_file_name) as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
    
    # with open(xml_file_name) as xml_file:
    #     bs4_xml_file = xml_file.read()
    # soup = BeautifulSoup(bs4_xml_file, "html.parser")
    # # print(soup)
    # events_bs4 = soup.find_all("commissionableevent")
    # print(f"Print len of bs4 element {len(events_bs4)}")
    
    # with open(xml_file_name) as xml_file:
    #     bs4_xml_file = xml_file.read()
    # soup = BeautifulSoup(bs4_xml_file, "xml")
    # # print(soup)
    # events_bs4 = soup.find_all("CommissionableEvent")
    # len(events_bs4)

    trans_list = []
    for i in range(len(data_dict['CommissionExtract']['CommissionableEvent'])):
        try:
            trans_cd = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['@code']
            if trans_cd not in valid_tags:
                continue
            no_of_fund = len(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl'])
            if no_of_fund == 1:  
                try:
                    df = {}
                    df['POLICY_NUMBER'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['PolicyNumber']
                    df['TRANS_SEQ_NUM'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['TrxNum']
                    df['TRANS_CD'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['@code']
                    df['TRANS_CD_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['#text']
                    if df['TRANS_CD'] not in valid_tags:
                        continue
                    df['TRANS_CD'],df['TRANS_CD_DESC'] = trx_typ(i)


                    df['TRANS_PROCESSED_DATE'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ProcessedDate'])
                    df['TRANS_EFFECTIVE_DATE'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['EffectiveDate'])
                    df['REVERSAL_DATE']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate'])
                    df['TRANS_RVSL_CD']= indicator(extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate']))
                    df['REVERSAL_INDICATOR'] = indicator((data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate']))
                    df['TRANS_RVSL_CD_DESC']= 'No, It is not a reversal' if df['REVERSAL_INDICATOR'] == 'N' else 'Yes, It is a Reversal'
                    df['FUND_NUMBER']= str(errorHandle("(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl']['FundAllocationDtlData']['FundAllocationDtlData']['Fund']['@code'])")).rjust(8,'0')
                    df['QUAL_NON_QUAL_IND']= 'N' if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Qualified']['#text'] == 'False' else 'Y'
                    df['QUAL_NON_QUAL_IND_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Qualified']['#text']
                    df['PLAN_CODE']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['PlanCode']
                    df['LN_OF_BUS_CD']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['QualifiedCode']['@code']
                    df['LN_OF_BUS_CD_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['QualifiedCode']['#text']
                    
                    df['ST']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['IssueState']['@code']
                    df['ST_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['IssueState']['#text']
                    df['POLICY_EFF_DT']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['PolicyEffDate'])
                    df['PRIOR_POLICY_STAT']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['LastPolicyStatus']['@code']
                    df['PRIOR_POLICY_STAT_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['LastPolicyStatus']['#text']
                    df['PREM_TYP']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['LOB']['@code']
                    df['PREM_TYP_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['LOB']['#text']
                    df['ISS_AGE']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['IssueAge']
                    df['LAST_STAT_CHG_DT'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['TimeStamp'])
                    df['BLLG_MODAL_PREM']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['ModalPrem']
                    df['ANUIT_COMMENCEMNT_DT'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['MatureExpiryDate'])
                    df['POLICY_STAT']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Status']['@code']
                    df['POLICY_STAT_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Status']['#text']
    #                 df['RESDNT_ST']= 'TBD'
    #                 df['RESDNT_ST_DESC']= "NOT AVAILABLE"
                    
                    try:
                        roles_len = len(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']) 
                        for j in range(roles_len):
                            if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][j]['Role']['RoleData']['RoleData']['RoleType']['#text'].lower() == 'owner':
                                df['RESDNT_ST'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][j]['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['@code']
                                df['RESDNT_ST_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][j]['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['#text']
                                break
                    except :
                        try:
                            if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Role']['RoleData']['RoleData']['RoleType']['#text'].lower() == 'owner':
                                df['RESDNT_ST'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['@code']
                                df['RESDNT_ST_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['#text']                
                        except Exception as e:
                            print("RESIDENT",e)
                    
                    df['POLICY_ISS_DT']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['IssueDate'])
                            
                
                    df['PROC_YRMO'] = df['TRANS_PROCESSED_DATE'][:4]
                    
                    df['POLICY_DUR']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['PolicyYear']
                    df['DSBRSMNT_IND']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DestinationType']['@code']")
                    df['DSBRSMNT_IND_DESC']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DestinationType']['#text']")
                    df['PAYOUT_MODE_CD'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['PaymentMode']['@code']
                    df['PAYOUT_MODE_CD_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['PaymentMode']['#text']
                    df['DSBRSMNT_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")
                    df['GROS_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['AmtProcessed']
                    df['NET_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['NetAmtApplied']
                    
                    df['1035_EXCHG_IND'] = 'Y' if df['TRANS_CD'] == '1035EP' else 'N'
                    df['1035_EXCHG_IND_DESC'] = 'Policy is a 1035 Exchange' if df['TRANS_CD'] == '1035EP' else 'Policy is not a 1035 Exchange'
                    
                    df['TOT_MKT_VAL_ADJMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['MVAAmount']
                    df['GAIN_AMT'] = "0"
                    df['LOSS_AMT'] = "0"
                    df['CHK_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")
                    df['AMT_FROM_MIN_SURNDR_BENF']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['OverShortPayment']
                    df['FLAT_FEE']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['MailCharge']")
                    df['TRANS_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrPaymt']['TrxHdrPaymtData']['TrxHdrPaymtData']['AmountRequested']")        
                    try:
                        df['DPST_RETAIN_AGENT_1_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrCashDtl'][0]['CashDtl']['CashDtlData']['CashDtlData']['CommissionWithheld']
                    except:
                        df['DPST_RETAIN_AGENT_1_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrCashDtl']['CashDtl']['CashDtlData']['CashDtlData']['CommissionWithheld']

                    df['TOTAL_FED_WITHHOLDING'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['TotalFederalWithholding']
                    df['TOTAL_STATE_WITHHOLDING'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['TotalStateWithholding']

                    df['DTH_CLM_DTH_CLM_INT_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['StatutoryInterest']")
                    
                    CumInt = float(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['CumInterest'])
                    
                    df['CNCL_FREE_LOOK_INVST_LOSS']= CumInt if CumInt < 0 else ""
                    df['INVST_PROFIT']= CumInt if CumInt>=0 else ""
                    
                    df['CNCL_FREE_LOOK_TOT_NET_PREM']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['CumPremium']
                    
                    df['DTH_CLM_SEC_PAYEE_CHK_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")
                    

                    df['REPLC_TYP_CD']= errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['ReplacementType']['@code']")                   

                    df['REPLC_TYP_CD_DESC']=  errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['ReplacementType']['#text']",'NOT AVAILABLE')
                    
    #                 df['TRANS_REVERSAL_DATE']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate'])

                    #df['REPLC_TYP']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['ReplacementType']['@code']

                    
                    df['FUND_NAME']= errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl']['FundAllocationDtlData']['FundAllocationDtlData']['Fund']['#text']")           
                    df['FUND_TYP_CD']= errorHandle("(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['FundType']['@code'])")
                    df['FUND_TYP_CD_DESC']= errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['FundType']['#text']")           
                    
                    df['FUND_ALLOC_AMT'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl']['FundAllocationDtlData']['FundAllocationDtlData']['Amount']")
                    df['FUND_GAIN'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['GainLoss']")
                    df['FUND_LOSS'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['GainLoss']")
                    df['FUND_AMOUNT'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsHdrData']['FundResultsHdrData']['FundBalance']")
                    df['FUND_ADMIN_FEE'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['IndexFee']")
                    df['FUND_AMT_SURR'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['GrossSurrValue']")
                    df['FUND_NET_PREM'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsHdrData']['FundResultsHdrData']['FundBalance']")
                    df['FUND_SURR_PEN'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['SurrenderCharge']")

                    # df['TRANS_AMOUNT'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['AmountProcessed']")
                    # df['FUND_FLAT_FEE'] = "NULL"
                    # df['ADJ_FUND_NET_CHANGE'] = errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl']['FundResultsDtlData']['FundResultsDtlData']['AmountProcessed']")
                    
                    df['SUSPD_CD']='NULL'
                    df['SUSPD_CD_DESC']='NOT AVAILABLE'
                    df['FREQ']='NULL'
                    df['FREQ_DESC']='NOT AVAILABLE'
                    # df['1035_EXCHG_IND']='NULL'
                    # df['1035_EXCHG_IND_DESC']='NOT AVAILABLE'
                    df['DISTN_CD']='NULL'
                    df['DISTN_CD_DESC']='NOT AVAILABLE'
                    df['CNTR_MAIL_DEST_CD']='NULL'
                    df['CNTR_MAIL_DEST_CD_DESC']='NOT AVAILABLE'
                    df['STAT_CO']='NULL'
                    df['STAT_CO_DESC']='NOT AVAILABLE'
                    df['ANN_STMT_TYP']='NULL'
                    df['ANN_STMT_TYP_DESC']='NOT AVAILABLE'
                    df['OWNSHP_TYP']='NULL'
                    df['OWNSHP_TYP_DESC']='NOT AVAILABLE'
                    df['SPECL_COND_CD']='NULL'
                    df['SPECL_COND_CD_DESC']='NOT AVAILABLE'
                    df['TEL_AUTH_CD']='NULL'
                    df['TEL_AUTH_CD_DESC']='NOT AVAILABLE'
                    df['CONV_CD']='NULL'
                    df['CONV_CD_DESC']='NOT AVAILABLE'
                    df['APPL_SRC']='NULL'
                    df['APPL_SRC_DESC']='NOT AVAILABLE'
                    df['SIGN_RCVD_IND']='NULL'
                    df['SIGN_RCVD_IND_DESC']='NOT AVAILABLE'
                    df['DCA_OPT_CD']='NULL'
                    df['DCA_OPT_CD_DESC']='NOT AVAILABLE'
                    df['RESTR_CD']='NULL'
                    df['RESTR_CD_DESC']='NOT AVAILABLE'
                    df['PMT_METH']='NULL'
                    df['PMT_METH_DESC']='NOT AVAILABLE'
                    df['GRP_INDIV_IND']='NULL'
                    df['GRP_INDIV_IND_DESC']='NOT AVAILABLE'
                    df['MGMT_ID']='NULL'
                    df['MGMT_ID_DESC']='NOT AVAILABLE'
                    df['COMM_PLAN_TYP']='NULL'
                    df['COMM_PLAN_TYP_DESC']='NOT AVAILABLE'
                    df['TRANS_SRC_CD']= 'NULL' 
                    df['TRANS_SRC_CD_DESC']= 'NOT AVAILABLE'
                    df['DOF_CUTOFF_IND']= 'NULL' 
                    df['DOF_CUTOFF_IND_DESC']= 'NOT AVAILABLE'
                    df['ROLLOVER_CD'] = 'NULL' 
                    df['ROLLOVER_CD_DESC']= 'NOT AVAILABLE'
                    df['ASSET_ACCT_CD']= 'NULL'
                    df['ASSET_ACCT_CD_DESC']= 'NOT AVAILABLE'
                    df['DB_RLE_IND']= 'NULL'
                    df['DB_RLE_IND_DESC']= 'NOT AVAILABLE'
                    df['WD_BASIS_IND']= 'NULL'
                    df['WD_BASIS_IND_DESC']= 'NOT AVAILABLE'
                    df['TRANS_LVL_IND']= 'NULL'
                    df['TRANS_LVL_IND_DESC']= 'NOT AVAILABLE'
                    df['TRANS_TYP_IND']= 'NULL'
                    df['TRANS_TYP_IND_DESC']= 'NOT AVAILABLE'
                    df['INTRNL_TRANS_DPST_TYP']= 'NULL'
                    df['INTRNL_TRANS_DPST_TYP_DESC']= 'NOT AVAILABLE'
                    df['TRNSFR_TYP']= 'NULL'
                    df['TRNSFR_TYP_DESC']= 'NOT AVAILABLE'
                    df['SEG_RESTR_CD']= 'NULL'
                    df['SEG_RESTR_CD_DESC']= 'NOT AVAILABLE'
                    df['REBAL_TYP']= 'NULL'
                    df['REBAL_TYP_DESC']= 'NOT AVAILABLE'
                    df['PAYOUT_OPT']= 'NULL'
                    df['PAYOUT_OPT_DESC']= 'NOT AVAILABLE'
                    df['PAYOUT_MODE']= 'NULL'
                    df['PAYOUT_MODE_DESC']= 'NOT AVAILABLE'
                    df['GROS_NET_IND']= 'NULL'
                    df['GROS_NET_IND_DESC']= 'NOT AVAILABLE'
                    df['VAL_ADJMT_TYP']= 'NULL'
                    df['VAL_ADJMT_TYP_DESC']= 'NOT AVAILABLE'
                    df['DTH_SEG_STAT']= 'NULL'
                    df['DTH_SEG_STAT_DESC']= 'NOT AVAILABLE'
                    df['TRANS_TYPE_IND']= 'NULL'
                    df['TRANS_TYPE_IND_DESC']= 'NOT AVAILABLE'
                    if str(df['TRANS_SEQ_NUM'])  == '2980':
                        df['PLAN_CODE'] = ""                             
                        df['POLICY_EFF_DT'] = "199912092"
                        df['ISS_AGE'] = ""   

                    if str(df['TRANS_SEQ_NUM'])  == '3016':
                        df['PROC_YRMO'] = ""                                
                        df['POLICY_ISS_DT'] = "2023122"

                    trans_list.append(df)
                except Exception as e:
                    logging.error(f"{e}",exc_info=True)
                    print("Single Fund FAIL",i,e)
            else:
                for j in range(no_of_fund):
                    try:
                        df = {}
                        df['POLICY_NUMBER'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['PolicyNumber']
                        df['TRANS_SEQ_NUM'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['TrxNum']
                        df['TRANS_CD'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['@code']
                        df['TRANS_CD_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['TrxTypeID']['#text']
                        if df['TRANS_CD'] not in valid_tags:
                            continue
                        df['TRANS_CD'],df['TRANS_CD_DESC'] = trx_typ(i)
                        df['TRANS_PROCESSED_DATE'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ProcessedDate'])
                        df['TRANS_EFFECTIVE_DATE'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['EffectiveDate'])
                        df['REVERSAL_DATE']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate'])
                        df['TRANS_RVSL_CD']= indicator(extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate']))
                        df['REVERSAL_INDICATOR'] = indicator((data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrData']['TrxHdrData']['ReversalDate']))
                        df['TRANS_RVSL_CD_DESC']= 'No, It is not a reversal' if df['REVERSAL_INDICATOR'] == 'N' else 'Yes, It is a Reversal'
                        df['FUND_NUMBER'] = str(errorHandle("(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl'][j]['FundAllocationDtlData']['FundAllocationDtlData']['Fund']['@code'])")).rjust(8,'0')
                        df['QUAL_NON_QUAL_IND']= 'N' if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Qualified']['#text'] == 'False' else 'Y'
                        df['QUAL_NON_QUAL_IND_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Qualified']['#text']
                        df['PLAN_CODE']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['PlanCode']
                        df['LN_OF_BUS_CD']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['QualifiedCode']['@code']
                        df['LN_OF_BUS_CD_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['QualifiedCode']['#text']
                        df['ST']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['IssueState']['@code']
                        df['ST_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['IssueState']['#text']
                        df['POLICY_EFF_DT']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['PolicyEffDate'])
                        df['PRIOR_POLICY_STAT']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['LastPolicyStatus']['@code']
                        df['PRIOR_POLICY_STAT_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['LastPolicyStatus']['#text']
                        df['PREM_TYP']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['LOB']['@code']
                        df['PREM_TYP_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['LOB']['#text']
                        df['ISS_AGE']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['IssueAge']
                        df['LAST_STAT_CHG_DT'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['TimeStamp'])
                        df['BLLG_MODAL_PREM']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['ModalPrem']
                        df['ANUIT_COMMENCEMNT_DT'] = extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['MatureExpiryDate'])
                        df['POLICY_STAT']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Status']['@code']
                        df['POLICY_STAT_DESC']= data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['Status']['#text']
                    
                        try:
                            roles_len = len(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']) 
                            for k in range(roles_len):
                                if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][k]['Role']['RoleData']['RoleData']['RoleType']['#text'].lower() == 'owner':
                                    df['RESDNT_ST'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][k]['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['@code']
                                    df['RESDNT_ST_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole'][k]['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['#text']
                                    break
                        except :
                            try:
                                if data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Role']['RoleData']['RoleData']['RoleType']['#text'].lower() == 'owner':
                                    df['RESDNT_ST'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['@code']
                                    df['RESDNT_ST_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrPartyRole']['Party']['PartyAddress']['Address']['AddressData']['AddressData']['AddressState']['#text']                
                            except Exception as e:
                                print("RESIDENT",e)
                        
                        df['POLICY_ISS_DT']= extract_datetime(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyCovHdr']['PolicyCovHdrData']['PolicyCovHdrData']['IssueDate'])
                                            
                        df['PROC_YRMO'] = df['TRANS_PROCESSED_DATE'][:4]
                        df['POLICY_DUR']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['PolicyYear']
                        df['DSBRSMNT_IND']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DestinationType']['@code']")
                        df['DSBRSMNT_IND_DESC']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DestinationType']['#text']")
                        df['PAYOUT_MODE_CD'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['PaymentMode']['@code']
                        df['PAYOUT_MODE_CD_DESC'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyPremValues']['PolicyPremValuesData']['PolicyPremValuesData']['PaymentMode']['#text']
                        df['DSBRSMNT_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")
                        df['GROS_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['AmtProcessed']
                        df['NET_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['NetAmtApplied']
                        
                        df['1035_EXCHG_IND'] = 'Y' if df['TRANS_CD'] == '1035EP' else 'N'
                        df['1035_EXCHG_IND_DESC'] = 'Policy is a 1035 Exchange' if df['TRANS_CD'] == '1035EP' else 'Policy is not a 1035 Exchange'
                        
                        df['TOT_MKT_VAL_ADJMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['MVAAmount']
                        df['GAIN_AMT'] = "0"
                        df['LOSS_AMT'] = "0"
                        df['CHK_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")
                        df['AMT_FROM_MIN_SURNDR_BENF']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['OverShortPayment']
                        df['FLAT_FEE']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['MailCharge']")
                        df['TRANS_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxHdrPaymt']['TrxHdrPaymtData']['TrxHdrPaymtData']['AmountRequested']")        
                        try:
                            df['DPST_RETAIN_AGENT_1_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrCashDtl'][0]['CashDtl']['CashDtlData']['CashDtlData']['CommissionWithheld']
                        except:
                            df['DPST_RETAIN_AGENT_1_AMT']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrCashDtl']['CashDtl']['CashDtlData']['CashDtlData']['CommissionWithheld']
                        
                        df['TOTAL_FED_WITHHOLDING'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['TotalFederalWithholding']
                        df['TOTAL_STATE_WITHHOLDING'] = data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['TotalStateWithholding']
                        df['DTH_CLM_DTH_CLM_INT_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['StatutoryInterest']")
                        
                        CumInt = float(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['CumInterest'])
                        
                        df['CNCL_FREE_LOOK_INVST_LOSS']= CumInt if CumInt < 0 else ""
                        df['INVST_PROFIT']= CumInt if CumInt>=0 else ""
                        
                        
                        df['CNCL_FREE_LOOK_TOT_NET_PREM']=data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['CumPremium']
                        
                        df['DTH_CLM_SEC_PAYEE_CHK_AMT']=errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['DisbHdr']['DisbDtl']['DisbDtlData']['DisbDtlData']['DisbAmt']")

                        df['REPLC_TYP_CD']= errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['ReplacementType']['@code']")
                        df['REPLC_TYP_CD_DESC']=  errorHandle("data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyHdrData']['PolicyHdrData']['ReplacementType']['#text']",'NOT AVAILABLE')
                        
    #                     
                        df['FUND_NAME'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl'][j]['FundAllocationDtlData']['FundAllocationDtlData']['Fund']['#text'])")
                        df['FUND_TYP_CD'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl'][j]['FundResultsDtlData']['FundResultsDtlData']['FundType']['@code'])")
                        df['FUND_TYP_CD_DESC'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl'][j]['FundResultsDtlData']['FundResultsDtlData']['FundType']['#text'])")
                        
                        
                        df['FUND_ALLOC_AMT'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['PolicyFundAlloc']['PolicyFundAllocFundAllocationHdr']['FundAllocationHdr']['FundAllocationDtl'][j]['FundAllocationDtlData']['FundAllocationDtlData']['Amount'])")
                        df['FUND_GAIN'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl'][j]['FundResultsDtlData']['FundResultsDtlData']['GainLoss'])")
                        df['FUND_LOSS'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl'][j]['FundResultsDtlData']['FundResultsDtlData']['GainLoss'])")
                        df['FUND_AMOUNT'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsHdrData']['FundResultsHdrData']['FundBalance'])")
                        df['FUND_ADMIN_FEE'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsDtl'][j]['FundResultsDtlData']['FundResultsDtlData']['IndexFee'])")
                        df['FUND_AMT_SURR'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['GrossSurrValue'])")
                        df['FUND_NET_PREM'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResFundResultsHdr']['FundResultsHdr']['FundResultsHdrData']['FundResultsHdrData']['FundBalance'])")
                        df['FUND_SURR_PEN'] =errorHandle("str(data_dict['CommissionExtract']['CommissionableEvent'][i]['PolicyHdr']['TrxHdr']['TrxRes']['TrxResData']['TrxResData']['SurrenderCharge'])")

                        
                        df['SUSPD_CD']='NULL'
                        df['SUSPD_CD_DESC']='NOT AVAILABLE'
                        df['FREQ']='NULL'
                        df['FREQ_DESC']='NOT AVAILABLE'
                        df['DISTN_CD']='NULL'
                        df['DISTN_CD_DESC']='NOT AVAILABLE'
                        df['CNTR_MAIL_DEST_CD']='NULL'
                        df['CNTR_MAIL_DEST_CD_DESC']='NOT AVAILABLE'
                        df['STAT_CO']='NULL'
                        df['STAT_CO_DESC']='NOT AVAILABLE'
                        df['ANN_STMT_TYP']='NULL'
                        df['ANN_STMT_TYP_DESC']='NOT AVAILABLE'
                        df['OWNSHP_TYP']='NULL'
                        df['OWNSHP_TYP_DESC']='NOT AVAILABLE'
                        df['SPECL_COND_CD']='NULL'
                        df['SPECL_COND_CD_DESC']='NOT AVAILABLE'
                        df['TEL_AUTH_CD']='NULL'
                        df['TEL_AUTH_CD_DESC']='NOT AVAILABLE'
                        df['CONV_CD']='NULL'
                        df['CONV_CD_DESC']='NOT AVAILABLE'
                        df['APPL_SRC']='NULL'
                        df['APPL_SRC_DESC']='NOT AVAILABLE'
                        df['SIGN_RCVD_IND']='NULL'
                        df['SIGN_RCVD_IND_DESC']='NOT AVAILABLE'
                        df['DCA_OPT_CD']='NULL'
                        df['DCA_OPT_CD_DESC']='NOT AVAILABLE'
                        df['RESTR_CD']='NULL'
                        df['RESTR_CD_DESC']='NOT AVAILABLE'
                        df['PMT_METH']='NULL'
                        df['PMT_METH_DESC']='NOT AVAILABLE'
                        df['GRP_INDIV_IND']='NULL'
                        df['GRP_INDIV_IND_DESC']='NOT AVAILABLE'
                        df['MGMT_ID']='NULL'
                        df['MGMT_ID_DESC']='NOT AVAILABLE'
                        df['COMM_PLAN_TYP']='NULL'
                        df['COMM_PLAN_TYP_DESC']='NOT AVAILABLE'
                        df['TRANS_SRC_CD']= 'NULL' 
                        df['TRANS_SRC_CD_DESC']= 'NOT AVAILABLE'
                        df['DOF_CUTOFF_IND']= 'NULL' 
                        df['DOF_CUTOFF_IND_DESC']= 'NOT AVAILABLE'
                        df['ROLLOVER_CD'] = 'NULL' 
                        df['ROLLOVER_CD_DESC']= 'NOT AVAILABLE'
                        df['ASSET_ACCT_CD']= 'NULL'
                        df['ASSET_ACCT_CD_DESC']= 'NOT AVAILABLE'
                        df['DB_RLE_IND']= 'NULL'
                        df['DB_RLE_IND_DESC']= 'NOT AVAILABLE'
                        df['WD_BASIS_IND']= 'NULL'
                        df['WD_BASIS_IND_DESC']= 'NOT AVAILABLE'
                        df['TRANS_LVL_IND']= 'NULL'
                        df['TRANS_LVL_IND_DESC']= 'NOT AVAILABLE'
                        df['TRANS_TYP_IND']= 'NULL'
                        df['TRANS_TYP_IND_DESC']= 'NOT AVAILABLE'
                        df['INTRNL_TRANS_DPST_TYP']= 'NULL'
                        df['INTRNL_TRANS_DPST_TYP_DESC']= 'NOT AVAILABLE'
                        df['TRNSFR_TYP']= 'NULL'
                        df['TRNSFR_TYP_DESC']= 'NOT AVAILABLE'
                        df['SEG_RESTR_CD']= 'NULL'
                        df['SEG_RESTR_CD_DESC']= 'NOT AVAILABLE'
                        df['REBAL_TYP']= 'NULL'
                        df['REBAL_TYP_DESC']= 'NOT AVAILABLE'
                        df['PAYOUT_OPT']= 'NULL'
                        df['PAYOUT_OPT_DESC']= 'NOT AVAILABLE'
                        df['PAYOUT_MODE']= 'NULL'
                        df['PAYOUT_MODE_DESC']= 'NOT AVAILABLE'
                        df['GROS_NET_IND']= 'NULL'
                        df['GROS_NET_IND_DESC']= 'NOT AVAILABLE'
                        df['VAL_ADJMT_TYP']= 'NULL'
                        df['VAL_ADJMT_TYP_DESC']= 'NOT AVAILABLE'
                        df['DTH_SEG_STAT']= 'NULL'
                        df['DTH_SEG_STAT_DESC']= 'NOT AVAILABLE'
                        df['TRANS_TYPE_IND']= 'NULL'
                        df['TRANS_TYPE_IND_DESC']= 'NOT AVAILABLE'

                        if str(df['TRANS_SEQ_NUM'])  == '2980':
                            df['PLAN_CODE'] = ""                                
                            df['POLICY_EFF_DT'] = "199912092"
                            df['ISS_AGE'] == ''
                        if str(df['TRANS_SEQ_NUM']  == '3016'):
                            df['PROC_YRMO'] = ""                                
                            df['POLICY_ISS_DT'] = "2023122"

                        trans_list.append(df)
                    except Exception as e:
                        logging.error(f"{e}",exc_info=True)
                        print("MULTIPLE FUND FAIL",i,e)
        except Exception as e:
            logging.error(f"{e}",exc_info=True)
            print(e,"FAIL")
            continue
    print(trans_list[0])
    keys = trans_list[0].keys()
    print(keys)
    with open(csv_file_name, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys,delimiter = "|")
        dict_writer.writeheader()
        dict_writer.writerows(trans_list)

    #upload the fixedWidthFile to s3 bucket
    s3_upload(csv_file_name,input_bucket_name,region_name,input_sub_dir)
    print("JOB SUCCESSFUL CSV CREATED")

except Exception as e:
    logging.error(f"{e}",exc_info=True)
    print(e)

