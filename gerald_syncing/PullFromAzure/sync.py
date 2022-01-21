##
# acts as the main function for comparing Gerald data with databases
##
import os
import json
import logging

from LV_db_connection import GremlinClient
from LV_db_connection import connect_sql_app
from LV_external_services import MSGraphClient

from gerald_syncing.PullFromAzure.azure import get_azure_employees
from gerald_syncing.PullFromAzure.gerald import format_queries
from gerald_syncing.PullFromAzure.sql import get_sql_employees, missing_sql_employees, sql_create_employees

"""
Class that pulls employees from Gerald, checks if all employees are accounted for and creates
new nodes for employees that do not exist in Gerald
"""
class PullFromAzureFacade():

    def __init__(self) -> None:
        gerald_username = os.environ['gerald_username']
        gerald_password=  os.environ['gerald_password']
        azure_username = os.environ['MSGraph_Client_ID']
        azure_password = os.environ['MSGraph_Secret']

        self.Gerald = GremlinClient(gerald_username, gerald_password)
        self.AzureAD = MSGraphClient(azure_username, azure_password)
        self.grcursor = connect_sql_app('GuaranteedRent')
        self.apprcursor = connect_sql_app('ArtemisDB')

    def pull(self):
        """
        Pulls employee information and stores it in a tmp file
        """

        #Employees in Azure AD
        azure_emps = get_azure_employees(self.AzureAD)
        
        with open("/tmpdata/Employees_azure.json", "w") as f:
            json.dump(azure_emps, f, indent=4)
    

        #Employees from the GR database
        gr_emps = get_sql_employees(self.grcursor, 'gr')
        with open("/tmpdata/Employees_gr.json", "w") as f:
            json.dump(gr_emps, f, indent=4)



    def process(self):
        """
        Formats queries for Gerald  and saves it to a tmp file
        """

        # Gerald Related Processing
        with open("/tmpdata/Employees_azure.json", "r") as f:
            gerald_emps = json.load(f)

        queries = format_queries(self.Gerald, gerald_emps)

        with open("/tmpdata/Employees_geraldqueries.json", "w") as f:
            json.dump(queries, f, indent=4)


        # GR Related Processing
        with open("/tmpdata/Employees_gr.json", "r") as f:
            gr_emps = json.load(f)
        
        gr_missing = missing_sql_employees(gerald_emps, gr_emps, database='gr')

        with open("/tmpdata/Employees_grmissing.json", "w") as f:
            json.dump(gr_missing, f, indent=4)


    def push_gerald(self):
        """
        Executes queries on Gerald
        """

        with open("/tmpdata/Employees_geraldqueries.json", "r") as f:
            queries = json.load(f)
        
        for query in queries:
            qstring = json.dumps(query, indent=4)
            try:
                self.Gerald.submit(query)
                # logging.info(f"Submitted queries for property {query['id']}: {qstring}")
                logging.info(f"Submitted queries for property {query[0]}: {qstring}")
            except Exception as e:
                #logging.error(f"Error submitting queries for {query['id']}: {qstring}", exc_info=True)
                logging.error(f"Error submitting queries for {query[0]}: {qstring}", exc_info=True)
        logging.info("Submitted all queries")

    def push_gr(self):
            """
            Executes queries on the GR Database
            """

            with open("/tmpdata/Employees_grmissing.json", "r") as f:
                gr_missing = json.load(f)

            sql_create_employees(self.grcursor, gr_missing, database='gr')

            logging.info("Submitted all queries")
