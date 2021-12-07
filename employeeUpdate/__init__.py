import logging
import requests
import os
from LV_external_services import azure
from LV_external_services import MSGraphClient
from LV_db_connection import gerald
import azure.functions as func

#returns all active employees 
def get_employees(AzureAD):
    employees = []
    employees_json = []

    r = requests.get('GET https://graph.microsoft.com/v1.0/users')
    
    # returns a json object of users
    tmp = AzureAD.retrieve_employees(os.environ["client_id"], os.environ["client_secret"])

    #a list of objects where each object is an employee containing their info - name, mail, id, licenses
    employees_json = tmp.values()
    for i in employees_json:
        #if statement to check if employee is still active
        # active status is decided by if employee has an assigned licenses - 0 licenses = not active 
        if (len(i['assignedLicenses']) == 0):
            i['active'] = False
        else:
            i['active'] = True
            employees.append(i)
           

    return employees


def format_queries(employees, gerald):
    empV = []
    for e in employees:
        # id, email, first name, last name, active 
        #the below query returns all associated properties of an object given an id
        query = f"g.V({e['id']})"
        
        #call to gerald 
        #if employee is not in gerald
        if len(query) == 0:
            new = f"g.addV({e.id}).property('firstName', {e['givenName']}).property(lastName, {e['surname']}).property('mail', {e['mail']}).property('active', {e['active']})"
            empV.append(new)
        else:
            new = f"g.addV({e.id}).property('firstName', {e['givenName']}).property(lastName, {e['surname']}).property('mail', {e['mail']}).property('active', {e['active' ]})"
            empV.append(query)
    return empV


def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    Gerald = gerald()
    AzureAD = azure()
    emp = get_employees(AzureAD)
    queries = format_queries(emp, Gerald)

    for query in queries:
        Gerald.submit(query)
