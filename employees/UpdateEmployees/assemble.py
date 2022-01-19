##
# acts as the main function for comparing Gerald data with databases
##

import json
import logging

from LV_db_connection import GremlinClient
#from LV_db_connection import connect_sql_app
from LV_external_services import MSGraphClient
from employees.UpdateEmployees.gerald import get_employees, format_queries

"""
Class that pulls employees from Gerald, checks if all employees are accounted for and creates
new nodes for employees that do not exist in Gerald
"""
class PullFromGeraldFacade():
    def __init__(self, gerald_username, gerald_password, db_username, db_password) -> None:
        self.Gerald = GremlinClient(gerald_username, gerald_password)
        self.AzureAD = MSGraphClient(db_username, db_password)
        #self.sql = connect_sql_app('GuaranteedRent')
    
    def pull(self):
        """
        Pulls information from Gerald and stores it in a tmp file
        """

        gerald_emps = get_employees(self)

        with open("/tmpdata/Employees_gerald.json", "w") as f:
            json.dump(gerald_emps, f)
    

    def process(self):
        """
        Forms the queries for Gerald and saves it to a tmp file
        """
        with open("/tmpdata/Employees_gerald.json", "r") as f:
            gerald_emps = json.load(f)

        queries = []
        queries = format_queries(gerald_emps)

        with open("/tmpdata/Employees_geraldqueries.json", "w") as f:
            json.dump(queries, f)

    def push(self):
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

