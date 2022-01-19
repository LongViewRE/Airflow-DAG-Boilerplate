def get_employees(employees_json):
    employees = []
        
    for i in employees_json:
        #if statement to check if employee is still active


        for key in i:
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

def format_queries(employees):
    empV = []
    for e in employees:
        # id, email, first name, last name, active 
        #the below query returns all associated properties of an object given an id
        query = f"g.V('emp-{e['id']}')"
        call = self.Gerald.submit(query)
        #call to gerald 
        #if employee is not in gerald
        if len(call) == 0:
            new = f"g.addV('emp-{e['id']}').property('firstName', '{e['givenName']}').property('lastName', '{e['surname']}').property('email', '{e['mail']}').property('active', '{e['active']}').property('mobile', '{e['mobilePhone']}')"
            empV.append(new)
        else:
            new = f"g.V('emp-{e['id']}').property('firstName', '{e['givenName']}').property('lastName', '{e['surname']}').property('email', '{e['mail']}').property('active', '{e['active' ]}').property('mobile', '{e['mobilePhone']}')"
            empV.append(query)
    return empV
