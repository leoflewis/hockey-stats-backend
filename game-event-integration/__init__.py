import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

import logging
import azure.functions as func
from datetime import datetime
from hockeylogic.ProcessGameEvents import ProcessGameEvents
from services.MYSQLService import MYSQLConnection

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Starting app')
    processor = ProcessGameEvents(MYSQLConnection())
    processor.ProcessSeason()