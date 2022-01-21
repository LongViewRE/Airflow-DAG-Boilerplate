def get_azure_employees(azure_client):
    '''
    Function to pull out employees from Azure and format them for later processing.
    '''
    employees = []

    employees_json = azure_client.retrieve_employees()['value']

    for i in employees_json:
        #if statement to check if employee is still active


        for key in i:
            if key == 'id':
                i[key] = 'emp-' + i[key]
            if i[key] is None:
                i[key] = ''
            if "'" in i[key]:
                i[key] = i[key].replace("'", "")
            
            # active status is decided by if employee has an assigned licenses - 0 licenses = not active 
        if (len(i['assignedLicenses']) == 0):
            i['active'] = False
        else:
            i['active'] = True

            
        #add only internal employees to table not external parties.
        if 'longview.com.au' in i['mail']:
            employees.append(i)
            
    return employees