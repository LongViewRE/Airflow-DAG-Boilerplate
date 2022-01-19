###############################################################################
# __main__.py                                                                 #
# Main driver file that parses command line args and executes whichever sync  #
# module is called                                                            #
###############################################################################
import os
import sys
import logging
import argparse

from gerald_syncing.PullFromAzure.sync import PullFromAzureFacade
from gerald_syncing.PullFromRPS.sync import PullFromRPSFacade

def main():
    
    #For command line arguments
    description = """This program syncs data to/from Gerald and various external sources"""
    modules = ["PullFromABX", "PullFromACC", "PullFromAzure", "PullFromIRE", 
                "PullFromRPS", "PullFromUBS", "PushToACC"]
    tasks = ["pull", "process", "push", "pushgerald", "pushgr"]

    # Init config/credentials
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
            format="%(asctime)s - %(message)s", datefmt='%d-%b-%y %H:%M:%S')
    rps_key = os.environ['rps_key']
    gerald_username = os.environ['gerald_username']
    gerald_password=  os.environ['gerald_password']
    azure_username = os.environ['MSGraph_Client_ID']
    azure_password = os.environ['MSGraph_Secret']


    # Parse command line arguments
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('module', choices=modules, help="the sync module to begin")
    parser.add_argument('task', choices=tasks, help="the task to execute")
    args = parser.parse_args()

    if args.module == "PullFromRPS":
        c = PullFromRPSFacade(rps_key, gerald_username, gerald_password)
        if args.task == "pull":
            c.pull()
        elif args.task == "process":
            c.process()
        elif args.task == "push":
            c.push()
    if args.module == "PullFromAzure":
        c = PullFromAzureFacade(gerald_username, gerald_password, azure_username, azure_password)
        if args.task == "pull":
            c.pull()
        elif args.task == "process":
            c.process()
        elif args.task == "pushgerald":
            c.push_gerald()
        elif args.task == 'pushgr':
            c.push_gr()
    else:
        print("Functionality not yet implemented")

if __name__ == "__main__":
    main()