import os
import sys
from merger import Merger
from labdataformatter import LabDataFormatter
from warning import WarningGenerator

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
                LabDataFormatter.build_xlab_csv_files(source, destination)

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
