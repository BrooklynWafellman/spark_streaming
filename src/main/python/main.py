import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1" # le venv est dans wsl donc pas accès au GPU

from pyspark.sql import SparkSession
from load import load_data
from train import train_model
from accuracy import evaluate
from tensorflow import keras
import configparser

config = configparser.ConfigParser()
config.read("../resources/config/config.ini")

spark = SparkSession.builder \
    .appName("Spark_mask") \
    .getOrCreate()

# ------ Load & Train ------
if config.getboolean("LOAD", "dataset_train", fallback=True):
    load_data(spark, config["PATH"]["input_train"],config["PATH"]["parquet_cache_train"])
    print("Train data loaded !")

if config.getboolean("LOAD", "model", fallback=False):
    model = keras.models.load_model(os.path.join(config["PATH"]["model_path"],config["PATH"]["model_name"]+".keras"))
else :
    model = train_model(config["PATH"]["parquet_cache_train"],config["PATH"]["model_path"],config["PATH"]["model_name"],epochs=config.getint("VARIABLE","epochs",fallback=100))
   

# ------ Test Load & Eval ------
if config.getboolean("LOAD", "dataset_test", fallback=True): 
    load_data(spark, config["PATH"]["input_train"],config["PATH"]["parquet_cache_test"])
    print("Test data loaded !")

# calcul accuracy val
evaluate(config["PATH"]["parquet_cache_test"], model)