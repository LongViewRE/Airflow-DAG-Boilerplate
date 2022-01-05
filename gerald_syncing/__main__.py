###############################################################################
# __main__.py                                                                 #
# Main driver file that parses command line args and executes whichever sync  #
# module is called                                                            #
###############################################################################
import sys
import logging
import argparse

import gerald_syncing.PullFromRPS.sync

def main():
    description = """This program syncs data to/from Gerald and various external sources"""
    modules = ["PullFromABX", "PullFromACC", "PullFromAzure", "PullFromIRE", 
                "PullFromRPS", "PullFromUBS", "PushToACC"]
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('module', choices=modules, help="the sync module to begin")

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    if args.module == "PullFromRPS":
        gerald_syncing.PullFromRPS.sync.main(1,2,3)
    else:
        print("Functionality not yet implemented")

if __name__ == "__main__":
    main()