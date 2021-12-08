import logging
import requests
import os
from dotenv import load_dotenv

from LV_external_services import MSGraphClient
from LV_db_connection import GremlinClient
import azure.functions as func

#returns all active employees 
def get_employees(AzureAD):
    employees = []
    employees_json = []

    # returns a json object of users
    tmp = AzureAD.retrieve_employees()

    #a list of objects where each object is an employee containing their info - name, mail, id, licenses
    employees_json = tmp['value']
    
    for i in employees_json:
        #if statement to check if employee is still active
        for key in i:
            if i[key] is None:
                i[key] = ''
        
        # active status is decided by if employee has an assigned licenses - 0 licenses = not active 
        if (len(i['assignedLicenses']) == 0):
            i['active'] = False
        else:
            i['active'] = True
        
        #add only internal employees to table not external parties.
        if 'longview.com.au' in i['mail']:
            employees.append(i)
           
    return employees


def format_queries(employees, gerald):
    empV = []
    for e in employees:
        # id, email, first name, last name, active 
        #the below query returns all associated properties of an object given an id
        query = f"g.V('emp-{e['id']}')"
        call = gerald.submit(query)
        #call to gerald 
        #if employee is not in gerald
        if len(call) == 0:
            new = f"g.addV('emp-{e['id']}').property('firstName', '{e['givenName']}').property('lastName', '{e['surname']}').property('email', '{e['mail']}').property('active', '{e['active']}').property('mobile', '{e['mobilePhone']}')"
            empV.append(new)
        else:
            new = f"g.V('emp-{e['id']}').property('firstName', {e['givenName']}).property('lastName', {e['surname']}).property('email', {e['mail']}).property('active', {e['active' ]}).property('mobile', '{e['mobilePhone']}')"
            empV.append(query)
    return empV


def main():

    load_dotenv()

    Gerald = GremlinClient(os.environ['gu'], os.environ['gp'])
    AzureAD = MSGraphClient(os.environ["gru"], os.environ["grs"])
    emp = get_employees(AzureAD)
    queries = format_queries(emp, Gerald)

    for query in queries:
        Gerald.submit(query)
