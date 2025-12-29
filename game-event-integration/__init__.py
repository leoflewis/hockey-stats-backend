import sys, os
import logging
import azure.functions as func
from datetime import datetime
from hockeylogic.ProcessGameEvents import ProcessGameEvents
from services.MYSQLService import MYSQLConnection

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Starting app')
    try:
        processor = ProcessGameEvents(MYSQLConnection())
        processor.ProcessSeason()
    except Exception as ex:
        logging.error("Error: %s", ex)