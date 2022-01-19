def appr_missing_employees(appr_emps, gerald_emps):
    emps_to_create = []

    for emp in gerald_emps:
        for appr_emp in appr_emps:
            if(emp['id'] == appr_emp['id']):
                emps_to_create.append(emp)
    return emps_to_create
    
def appr_create_employees(emps_to_create) -> None:
    #SQL QUERY  
    for emp in emps_to_create:
        f"INSERT INTO TABLE employees"
        f"VALUES (emp['id']), (emp['firstName']), (emp['lastName']), (emp['email'])"