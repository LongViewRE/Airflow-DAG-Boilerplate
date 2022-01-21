def get_sql_employees(db_client, database):
    '''
    Function to retrieve sql employees. 
    '''

    emps = []

    # Query to Database
    if database == 'gr':
        tmp_emps = db_client.execute('SELECT employeeID, firstName, lastName, emailID FROM employees')
    
    elif database == 'appraisals':
        tmp_emps = db_client.execute('SELECT employeeID, firstName, lastName, emailID FROM employees')
    
    #formatting data outputs
    for emp in tmp_emps:
        data = {
            'id': emp[0],
            'firstName': emp[1],
            'lastName': emp[2],
            'email': emp[3]
        }
        emps.append(data)
    
    return emps


def missing_sql_employees(azure_emps, gr_emps = [], appr_emps = [], database = 'gr'):
    '''
    Function to compare employees from Azure with various databases.
    '''
    emps_to_create = []

    if database == 'gr':

        for emp in azure_emps:
            for gr_emp in gr_emps:
                if(emp['mail'] == gr_emp['email']):
                    emps_to_create.append(emp)


    elif database == 'appraisals':

        for emp in azure_emps:
            for appr_emp in appr_emps:
                if(emp['id'] == appr_emp['id']):
                    emps_to_create.append(emp)

    return emps_to_create



def sql_create_employees(db_client, emps_to_create, database) -> None:
    '''
    Function to update sql employees table.
    '''
    if database == 'gr':

        for emp in emps_to_create:
                db_client.execute(f"INSERT INTO TABLE employees VALUES ('{emp['id']}', '{emp['givenName']}', '{emp['surname']}', '{emp['mail']}')")
    
    elif database == 'appraisals':

        for emp in emps_to_create:
                db_client.execute(f"INSERT INTO TABLE employees VALUES ('{emp['id']}', '{emp['firstName']}', '{emp['lastName']}', '{emp['email']}')")
    