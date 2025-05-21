import os

class DailyTansactionsChecking:
    """
    This class will check on the repertory dailytrans to detect new transactions csv file
    and log them in a file dailytrans_log.txt
    
    """
    
    
    def __init__(self, path_dailytrans_repertory, path_dailytrans_logger):
        self.path_dailytrans_repertory = path_dailytrans_repertory
        self.path_dailytrans_logger = path_dailytrans_logger

    def check_on_dailytrans_repertory(self):
        """Check the daily transaction repertory to detect new CSV transaction files"""

        # List CSV files in the repertory
        list_repertory_dailytrans = [i for i in os.listdir(self.path_dailytrans_repertory) if i.endswith('.csv')]
        set_repertory_dailytrans = set(list_repertory_dailytrans)

        # Read the existing logged files
        if os.path.exists(self.path_dailytrans_logger):
            with open(self.path_dailytrans_logger, 'r') as f:
                list_logger_dailytrans = f.read().splitlines()
        else:
            list_logger_dailytrans = []

        set_logger_dailytrans = set(list_logger_dailytrans)

        # Compare sets
        set_comparing = set_repertory_dailytrans - set_logger_dailytrans
        list_comparing = list(set_comparing)

        if len(list_comparing) > 0:
            # Log the new files
            with open(self.path_dailytrans_logger, 'a') as f:
                for i in list_comparing:
                    f.write(i + '\n')

            print(f'>>> New transactions file(s) detected : {list_comparing[-1]} >>>')
            return {'new' : True, 'filename' : list_comparing[-1]}

        return {'new' : False, 'filename' : None}
