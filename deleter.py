import os

class Deleter:
    """Deleter class has two methods for deleting files.
    Currently unused but we have the option if needed"""

    # currently not using this method
    @staticmethod
    def delete_temp_xlab_csv_files(source: str) -> None:

        xlab_files = ["hall_xlab.csv", "icp_xlab.csv"]
        for file in xlab_files:
            if os.path.isfile(os.path.join(source, file)):
                os.remove(os.path.join(source, file))
        return

    # currently not using this method
    @staticmethod
    def delete_current_master_csv_file(source: str) -> None:

        # define filename
        master_csv = "X-Materials_master_data.csv"

        # if the file exists, remove it
        if os.path.isfile(os.path.join(source, master_csv)):
            os.remove(os.path.join(source, master_csv))
        return

