def format_queries(gerald_client, employees):
    '''
    Function to format employee queries for Gerald. 
    '''
    empV = []
    for e in employees:
        # id, email, first name, last name, active 
        #the below query returns all associated properties of an object given an id
        query = f"g.V('emp-{e['id']}')"
        call = gerald_client.submit(query)
        #call to gerald 
        #if employee is not in gerald
        if len(call) == 0:
            new = f"g.addV('emp-{e['id']}').property('firstName', '{e['givenName']}').property('lastName', '{e['surname']}').property('email', '{e['mail']}').property('active', '{e['active']}').property('mobile', '{e['mobilePhone']}')"
            empV.append(new)
        else:
            new = f"g.V('emp-{e['id']}').property('firstName', '{e['givenName']}').property('lastName', '{e['surname']}').property('email', '{e['mail']}').property('active', '{e['active' ]}').property('mobile', '{e['mobilePhone']}')"
            empV.append(query)
    return empV


