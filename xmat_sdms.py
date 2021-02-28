# 02/25/2021 Matthew Kinzler for Citrine Informatics
import re
import os
import sys
import glob
import pandas as pd
import petl as etl
from pandas import DataFrame
from merger import Merger
from xlabdataengine import XLabDataEngine




class Runner:
    """This is the runner class for the command line interface
    that makes use of a single static method. Self explanatory,
    with conditional logic and a few loops to get input.
    Messy and should probably be broken into functions"""

    @staticmethod
    def cmd_line_interface() -> None:

        print("Welcome to the Simple Data Management System!")

        print("Please enter the source (path where files to merge are located) now:\n")
        source = input()

        # input validation
        if not os.path.isdir(source):
            print(f"{source} is not a valid path.\n\nPlease double check your path "
                  f"and try running the app again.")
            sys.exit()

        print("Please enter the destination (path to save files) now:\n")
        destination = input()

        # input validation
        if not os.path.isdir(destination):
            print(f"{destination} is not a valid path.\n\nPlease double check your path "
                  f"and try running the app again.")
            sys.exit()

        # print source and destination to stdout
        print(f"Your source path is: \n{source}\n\n and destination path is: \n{destination}\n")

        # warning if source and destination match
        if source == destination:
            print(WarningGenerator.warning)
            print("Source and destination are the same path!\n")
        print("Is this correct? Type [y]es or [n]o.")

        answer = input()
        affirmative = ["yes", "Yes", 'YES', 'y', 'Y']  # definitely should use regex match here

        # conditional do stuff
        if answer in affirmative:
            try:
                print("Generating master.csv file now...\n")
                # generate X-lab csv files
                XLabDataEngine.build_xlab_csv_files(source, destination)

                # throw warnings if necessary
                WarningGenerator.warn_if_master_in_source_path(source)
                WarningGenerator.warn_if_master_in_destination_path(destination)

                # run the merger, merge *csv files in source, write output master csv to destination
                Merger.merge_csv(source, destination)

                print(f"Done.\nX-Materials_master_data.csv file "
                      f"written to: \n{destination}\nExiting. Have a nice day!")

            # catch both index and value errors together in this tuple since the warning is the same
            except (IndexError, ValueError):
                print(WarningGenerator.warning)
                print(f"Cannot continue. Possibly missing data in your source path."
                      f"\nPlease double check your source path. It"
                      f" is currently set to: \n\n{source}\n")
                sys.exit(-1)

            # warning if trying to work write files already open
            except PermissionError:
                print(WarningGenerator.warning)
                print(f"Cannot continue.\n"
                      f"Please close all CSV and TXT files in source path, and try again.")
                sys.exit(0)
        else:
            print(WarningGenerator.warning)
            print("Need to confirm that it's OK for source and destination path to match.")
            print("Exiting for safety!")
            sys.exit()

        return


def main():
    # run it!
    Runner.cmd_line_interface()


if __name__ == '__main__':
    main()

'''
########################
###### CHANGE LOG ######
########################

02/27/2021 
refactored Merger.merge_csv() to fix a bug on line 26. Variable file_to_remove should 
return a filename, not a boolean. This section of the method .merge_csv() should generate a list of files
to merge, and if one of the files in the list is the master CSV, remove it from the list. I put this in
as an extra check to prevent the code from merging a master CSV file with other CSV files in the source path.
Line 26 should return the file name, so that we can iterate over the list of files to merge, and remove that filename 
from the list if the filename exists in the list. Prior to refactor, line 26 was checking if the 
file exists (True or False), then attempting to use that boolean value in the check for files
to merge - (i.e. find True or False in the list files_to_merge) - which is nonsense in this context.

Note that this bug had no impact on the code, because the method WarningGenerator.warn_if_master_in_source_path() 
forces the program to quit if a master CSV is found in the source path, and WarningGenerator.warn_if_master_in_source_path() 
is called before Merger.merge_csv() in the runner. Thus the program will quit before Merger.merge_csv() is executed 
if the master CSV file is found in the source path.
 '''