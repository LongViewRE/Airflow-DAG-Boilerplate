##
# acts as the main function for comparing Gerald data with databases
##

import json
import logging

from LV_db_connection import GremlinClient
from LV_db_connection import connect_sql_app
from LV_external_services import MSGraphClient

from gerald_syncing.PullFromAzure.gerald import get_employees, format_queries
from gerald_syncing.PullFromAzure.update_gr import gr_create_employees, gr_missing_employees
from gerald_syncing.PullFromAzure.update_artemis import appr_create_employees, appr_missing_employees

"""
Class that pulls employees from Gerald, checks if all employees are accounted for and creates
new nodes for employees that do not exist in Gerald
"""
class PullFromAzureFacade():
    def __init__(self, gerald_username, gerald_password, azure_username, azure_password) -> None:
        self.Gerald = GremlinClient(gerald_username, gerald_password)
        self.AzureAD = MSGraphClient(azure_username, azure_password)
        self.grcursor = connect_sql_app('GuaranteedRent')
    
    def pull(self):
        """
        Pulls information from Gerald and stores it in a tmp file
        """

        gerald_emps = get_employees(self)

        with open("/tmpdata/Employees_gerald.json", "w") as f:
            json.dump(gerald_emps, f, indent=4)
    
        gr_emps = self.grcursor.execute("SELECT * FROM employees")
        with open("/tmpdata/Employees_gr.json", "w") as f:
            json.dump(gr_emps, f, indent=4)

    def process(self):
        """
        Forms the queries for Gerald and saves it to a tmp file
        """
        with open("/tmpdata/Employees_gerald.json", "r") as f:
            gerald_emps = json.load(f)

        queries = []
        queries = format_queries(gerald_emps)

        with open("/tmpdata/Employees_geraldqueries.json", "w") as f:
            json.dump(queries, f, indent=4)


        with open("/tmpdata/Employees_gr.json", "r") as f:
            gr_emps = json.load(f)
        
        gr_missing = gr_missing_employees(gr_emps, gerald_emps)
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
                logging.info(f"Submitted queries for property {query['id']}: {qstring}")
            except Exception as e:
                logging.error(f"Error submitting queries for {query['id']}: {qstring}", exc_info=True)

        logging.info("Submitted all queries")

    def push_gr(self):
            """
            Executes queries on the GR Database
            """

            with open("/tmpdata/Employees_grmissing.json", "r") as f:
                gr_missing = json.load(f)

            gr_create_employees(gr_missing, self.grcursor)
            logging.info("Submitted all queries")