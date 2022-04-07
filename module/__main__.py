###############################################################################
# __main__.py                                                                 #
# Main driver file that parses command line args and executes whichever sync  #
# module is called                                                            #
###############################################################################
import os
import sys
import logging
import argparse

##### CHANGE
# Import the class from the module you've written. Airflow prefers absolute imports.
from module.submodule.sync import Class

def main():
    
    #For command line arguments
    description = ""

    ###### CHANGE
    # Change these modules to include your submodule. 
    modules = ["Submodule"]

    ##### CHANGE
    # Change these tasks to the methods you define in your submodule class defined in submodule/sync.py
    tasks = ["pull", "process", "push"]

    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
            format="%(asctime)s - %(message)s", datefmt='%d-%b-%y %H:%M:%S')

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('module', choices=modules, help="the sync module to begin")
    parser.add_argument('task', choices=tasks, help="the task to execute")
    args = parser.parse_args()

    #### CHANGE
    # Adjust the module and tasks to what you define in submodule and tasks from the class.
    if args.module == "Submodule":

        #### CHANGE
        # Change the class to what you define in the submodule. 
        c = Class()

        #### CHANGE
        # Adjust tasks and methods to what you define in your submodule.
        if args.task == "pull":
            c.pull()
        elif args.task == "process":
            c.process()
        elif args.task == "push":
            c.push()

    else:
        print("Functionality not yet implemented")

if __name__ == "__main__":
    main()
