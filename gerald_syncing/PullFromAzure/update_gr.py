def gr_missing_employees(gr_emps, gerald_emps):
    emps_to_create = []

    for emp in gerald_emps:
        for gr_emp in gr_emps:
            if(emp['email'] == gr_emp['email']):
                emps_to_create.append(emp)
    return emps_to_create
    
def gr_create_employees(emps_to_create, cursor) -> None:
    #SQL QUERY  
   for emp in emps_to_create:
        cursor.execute("INSERT INTO TABLE employees VALUES (emp['id']), (emp['firstName']), (emp['lastName']), (emp['email'])")
