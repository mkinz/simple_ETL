import os
import glob
import sys
import pandas as pd

class Merger:
    """Merger class with single static method
    for merging all the CSV files together.
    It has built in protection to ensure that we
    do not join a master.csv file into the final
    master.csv file"""

    # none of these methods change the state of the object or class, so they are static
    def merge_csv(self, source: str, destination: str):

        # generate list of files to merge
        files_to_merge = glob.glob(os.path.join(source, "*csv"))

        # these next steps ensure we aren't merging a master.csv file with other files to make a new master.csv

        # NOTE: The method WarningGenerator.warn_if_master_in_source_path()
        # will force the program to exit if a master CSV is found in the source path. Furthermore,
        # WarningGenerator.warn_if_master_in_source_path() is called in the runner before Merger.merge_csv(),
        # so lines 31-35 are redundant and can be safely removed in a future release.
        # Leaving it in as it was part of my original submission on 02/26
        file_to_remove = os.path.join(source, "X-Materials_master_data.csv")

        # if the master CSV file is in the list files_to_merge, remove it
        if file_to_remove in files_to_merge:
            files_to_merge.remove(file_to_remove)

        # get a list of the file names that will be merged
        file_names = [os.path.basename(source) for source in files_to_merge]

        # show the names of files to merge, and confirm
        print("The following files will be merged:\n")

        for name in file_names:
            print(name)

        print("\nConfirm merge? Type [y]es or [n]o.")

        affirmative = ["yes", "Yes", 'YES', 'y', 'Y']  # definitely should be a regex match

        answer = input()

        if answer in affirmative:

            # create list of dataframe objects from files in files_to_merge
            dfs = [pd.read_csv(file) for file in files_to_merge]

            # join the dataframes together with an outer join, then save it as the master.csv file
            merged_dataframe = pd.concat(dfs, axis=1, join='outer') \
                .to_csv(os.path.join(destination, "X-Materials_master_data.csv"))

            return merged_dataframe
        else:
            print("Exiting without doing anything.")
            sys.exit(0)
