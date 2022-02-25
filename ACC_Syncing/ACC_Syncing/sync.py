##
# acts as the main function for comparing Gerald data with databases
##

from LV_external_services import
"""
Class that pulls employees from Gerald, checks if all employees are accounted for and creates
new nodes for employees that do not exist in Gerald
"""
class ACCSyncingClientFacade():

    def __init__(self) -> None:


    def pull(self):
        """
        Pulls employee information and stores it in a tmp file
        """

        with open("/tmpdata/Employees_azure.json", "w") as f:
            json.dump(azure_emps, f, indent=4)
    

        with open("/tmpdata/Employees_appr.json", "w") as f:
            json.dump(appr_emps, f, indent=4)



    def process_blob(self):
        """
        Formats queries for Gerald  and saves it to a tmp file
        """

        with open("/tmpdata/Employees_azure.json", "r") as f:
            gerald_emps = json.load(f)


        with open("/tmpdata/Employees_geraldqueries.json", "w") as f:
            json.dump(queries, f, indent=4)


        # GR Related Processing
        with open("/tmpdata/Employees_gr.json", "r") as f:
            gr_emps = json.load(f)

        with open("/tmpdata/Employees_grmissing.json", "w") as f:
            json.dump(gr_missing, f, indent=4)

        # Appraisals Related Processing
        with open("/tmpdata/Employees_appr.json", "r") as f:
            appr_emps = json.load(f)
        

        with open("/tmpdata/Employees_apprmissing.json", "w") as f:
            json.dump(appr_missing, f, indent=4)        


    def push_gerald(self):
        """
        Executes queries on Gerald
        """

        with open("/tmpdata/Employees_geraldqueries.json", "r") as f:
            queries = json.load(f)
        
        for query in queries:
            qstring = json.dumps(query, indent=4)
            try:
                self.Gerald.submit(query)
                # logging.info(f"Submitted queries for property {query['id']}: {qstring}")
                logging.info(f"Submitted queries for property {query[0]}: {qstring}")
            except Exception as e:
                #logging.error(f"Error submitting queries for {query['id']}: {qstring}", exc_info=True)
                logging.error(f"Error submitting queries for {query[0]}: {qstring}", exc_info=True)
        logging.info("Submitted all queries")
