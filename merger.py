import os
import glob
import sys
import pandas as pd

class Merger:
    """Merger class with single method for merging all the CSV files together.
    It has built in protection to ensure that we do not join a master.csv file into the final
    master.csv file"""

    # none of these methods change the state of the object or class, so they are static
    def merge_csv(self, source: str, destination: str):

        files_to_merge = glob.glob(os.path.join(source, "*csv"))

        file_to_remove = os.path.join(source, "Materials_master_data.csv")

        if file_to_remove in files_to_merge:
            files_to_merge.remove(file_to_remove)

        file_names = [os.path.basename(source) for source in files_to_merge]

        print("The following files will be merged:\n")

        for name in file_names:
            print(name)

        print("\nConfirm merge? Type [y]es or [n]o.")

        affirmative = ["yes", "Yes", 'YES', 'y', 'Y']  # definitely should be a regex match

        answer = input()

        if answer in affirmative:

            dfs = [pd.read_csv(file) for file in files_to_merge]

            # join the dataframes together with an outer join, then save it as the master.csv file
            merged_dataframe = pd.concat(dfs, axis=1, join='outer') \
                .to_csv(os.path.join(destination, "Materials_master_data.csv"))

            return merged_dataframe
        else:
            print("Exiting without doing anything.")
            sys.exit(0)
