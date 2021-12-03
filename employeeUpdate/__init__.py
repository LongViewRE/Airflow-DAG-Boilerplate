import logging
import requests
import json

import azure.functions as func

#returns all active employees 
def retrieve_employees(AzureAD):
    employees = []

    r = requests.get('GET https://graph.microsoft.com/v1.0/users')
    AzureAD.get_employees()
    # returns a json object of users
    tmp = json.loads(r)

    #0th element of each json object is the display name, 1st element of each json object is their email

    for i in tmp:
        #if statement to check if employee is still active 
        if ((i['Groups'] == 'Operations') or (i['Groups'] == 'Operations - BYOD') or (i['Groups'] == 'Operations - Casual')):
            i['active'] = True
            employees.append(i)
        else:
            i['active'] = False

    return employees


def format_queries(employees, gerald):
    empV = []
    for e in employees:
        # id, email, first name, last name, active 
        #the below query returns all associated properties of an object given an id
        query = f"g.V({e['id']}).properties()"
        
        #call to gerald 
        if len(query) == 0:
            new = f"g.addV({e.id}).property().property().property('active', '{e['active']}')"
            empV.append(new)
        else:
            new = f"g.V('{e['id']}').property('firstName','{e["firstName"]}').property('lastName','').property('active', '{e['active']}')"
            empV.append(query)
    return empV


def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    gerald = Gerald()
    azure = AzureAD()

    emp = retrieve_employees(azure)
    queries = format_queries(emp, gerald)

    for query in queries:
        gerald.submit(query)
