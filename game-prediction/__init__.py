import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

import logging
import azure.functions as func
from hockeylogic.PredictGames import GamePredictionEngine

def main(mytimer: func.TimerRequest) -> None:

    engine = GamePredictionEngine()
    engine.ProcessGames()

    logging.info("Predicted tomorrows games")
