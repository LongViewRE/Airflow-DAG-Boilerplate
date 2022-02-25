######################################################################
# acts as the main function for comparing Gerald data with databases #
######################################################################
import json

#### CHANGE
# Adjust this class name to better reflect the name of your submodule.
class SubmoduleFacade():
    """
    What does this class do? 
    """

    def __init__(self) -> None:


    def pull(self):
        """
        What does this do?
        """

        function_response = function()

        ####CHANGE
        # Use /tmpdata/ to write data that can be accessible between functions. 
        with open("/tmpdata/something.json", "w") as f:
            json.dump(function_response, f, indent=4)



    def process(self):
        """
        What does this do?
        """

        #### CHANGE
        # use /tmpdata/ to access data from previous tasks. 
        with open("/tmpdata/Employees_azure.json", "r") as f:
            gerald_emps = json.load(f)



