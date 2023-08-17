from result_output import *
import sys
import json
import importlib.util
import urllib.request
from google.oauth2 import service_account
from pprint import pprint
from google.cloud import logging

class Activity():

    def testcase_check_BigQuery_Dataset_Name(self,test_object,credentials,project_id):
        testcase_description="Check BigQuery dataset name"
        expected_result="demo_bq"
        
        try:
            is_present = False
            actual = 'BigQuery dataset name is not '+ expected_result
            logger_name = "cloudaudit.googleapis.com%2Factivity"
            logging_client = logging.Client(credentials=credentials)

            try:
                logger = logging_client.logger(logger_name)

                log_filter = (
                    f'resource.type="bigquery_dataset" '
                    f'resource.labels.dataset_id="{expected_result}" '
                )

                for entry in logger.list_entries(filter_=log_filter, page_size=10):
                    if (entry.resource.labels["dataset_id"] == expected_result):
                        is_present=True
                        actual=expected_result
                        break

            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description,expected_result)
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"Congrats! You have done it right!"," ") 
            else:
                return test_object.update_result(0,expected_result,actual,"Check BigQuery Dataset name","https://cloud.google.com/bigquery/docs/datasets-intro")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_cloud_run_service_name"]=str(e)                

    def testcase_check_BigQuery_Table_Name(self,test_object,credentials,project_id):
        testcase_description="Check Bigquery Table name"
        expected_result="demo_table"
        
        try:
            is_present = False
            actual = 'BigQuery table is not '+ expected_result
            logger_name = "cloudaudit.googleapis.com%2Factivity"
            logging_client = logging.Client(credentials=credentials)
            try:
                logger = logging_client.logger(logger_name)

                log_filter = (
                    f'resource.type="bigquery_dataset" '
                )

                for entry in logger.list_entries(filter_=log_filter, page_size=10):
                    try:
                        if expected_result in (((entry.payload['metadata'])['tableCreation'])['table'])['tableName']:
                            is_present=True
                            actual=expected_result
                            break
                    except Exception as e1:
                        pass
            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description,expected_result)
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"Congrats! You have done it right!"," ") 
            else:
                return test_object.update_result(0,expected_result,actual,"Check BigQuery table Name","https://cloud.google.com/bigquery/docs/tables-intro")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_cloud_run_service_name"]=str(e)                

    def testcase_check_BigQuery_View_Name(self,test_object,credentials,project_id):
        testcase_description="Check Bigquery View name"
        expected_result="top_10_technologies"

        try:
            is_present = False
            actual = 'BigQuery table is not '+ expected_result
            logger_name = "cloudaudit.googleapis.com%2Factivity"
            logging_client = logging.Client(credentials=credentials)
            try:
                logger = logging_client.logger(logger_name)

                log_filter = (
                    f'resource.type="bigquery_dataset" '
                )

                for entry in logger.list_entries(filter_=log_filter, page_size=10):
                    try:
                        if expected_result in (((entry.payload['metadata'])['tableCreation'])['table'])['tableName']:
                            is_present=True
                            actual=expected_result
                            break
                    except Exception as e1:
                        pass

            except Exception as e:
                is_present = False

            test_object.update_pre_result(testcase_description,expected_result)
            if is_present==True:
                test_object.update_result(1,expected_result,actual,"Congrats! You have done it right!"," ") 
            else:
                return test_object.update_result(0,expected_result,actual,"Check BigQuery View Name","https://cloud.google.com/bigquery/docs/views-intro")   

        except Exception as e:    
            test_object.update_result(-1,expected_result,"Internal Server error","Please check with Admin","")
            test_object.eval_message["testcase_check_cloud_run_service_name"]=str(e)                

def start_tests(credentials, project_id, args):

    if "result_output" not in sys.modules:
        importlib.import_module("result_output")
    else:
        importlib.reload(sys.modules[ "result_output"])
    
    test_object=ResultOutput(args,Activity)
    challenge_test=Activity()
    challenge_test.testcase_check_BigQuery_Dataset_Name(test_object,credentials,project_id)
    challenge_test.testcase_check_BigQuery_Table_Name(test_object,credentials,project_id)
    challenge_test.testcase_check_BigQuery_View_Name(test_object,credentials,project_id)

    json.dumps(test_object.result_final(),indent=4)
    return test_object.result_final()

