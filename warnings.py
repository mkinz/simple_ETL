import os
import sys

class WarningGenerator:
    """Warning class to give useful warnings when needed"""

    # static variable
    warning = "WARNING"

    def warn_if_master_in_source_path(sefl, source: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        # define filename
        master_csv = "X-Materials_master_data.csv"

        # if found, print a warning message
        if os.path.isfile(os.path.join(source, master_csv)):
            print("It looks like you already have a "
                  "master CSV file in your source directory.\n"
                  "This will cause problems. Please remove it and try again.")
            sys.exit(0)
        return

    def warn_if_master_in_destination_path(self, destination: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        # define filename
        master_csv = "X-Materials_master_data.csv"
        if os.path.isfile(os.path.join(destination, master_csv)):

            # if found, print a warning message, ask if overwrite is ok
            print("Master CSV file already exists in destination path.\n"
                  "OK to overwrite?\n"
                  "You can select [y]es or [n]o")

            affirmative = ["yes", "Yes", 'YES', 'y', 'Y']

            answer = input()

            # if overwrite is ok, then continue back to main runner
            if answer in affirmative:
                pass

            # otherwise, get the heck out of here
            else:
                print("Exiting without doing anything.")
                sys.exit(0)
        return

