import pandas as pd

class DbInterface:
    """
    Interface for connecting and pushing data to a database.
    """
    def push_data_to_database(self, connection, datastream):
        pass

    def get_dat_from_database(self, connection, datastream):
        pass