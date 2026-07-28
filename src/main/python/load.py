from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, BinaryType, IntegerType, StructField
import numpy as np
from PIL import Image
import io
from operations import preprocess
from kernel import kernel


def convert_img(content):
    img = np.array(Image.open(io.BytesIO(content))).astype(np.float32)
    img.resize((100,100,3)) # resize parce que les images ont pas toutes la meme taille
    img_processed = preprocess(img, kernel.blur, grayscale=False)
    img_processed = preprocess(img_processed, kernel.edge_detection_3, grayscale=True, offset=128)
    return (
        content,
        list(img.shape),
        bytes(img_processed.tobytes()),
        list(img_processed.shape)
    )


convert_udf = F.udf(convert_img, StructType([
    StructField("original", BinaryType()),
    StructField("original_shape", ArrayType(IntegerType())),
    StructField("processed", BinaryType()),
    StructField("processed_shape", ArrayType(IntegerType()))
]))


def load_data(spark: SparkSession, data_path: str, save_path : str):
    df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.{png,PNG,jpg,JPG}") \
        .option("recursiveFileLookup", "true") \
        .load(data_path)

    df = df.select(
        convert_udf(F.col("content")).alias("image"),
        F.element_at(F.split(df["path"], "/"), -2).alias("label"),
        F.col("path")
    )

    df = df.select(
        F.col("path"),
        F.col("image.original").alias("original"),
        F.col("image.original_shape").alias("original_shape"),
        F.col("image.processed").alias("processed"),
        F.col("image.processed_shape").alias("processed_shape"),
        F.col("label"),
        F.when(F.col("label") == "WithMask", 1).otherwise(0).alias("label_index"), #la classe match avec scala (faire attention si on veux un changement quelconque)
    )

    df.repartition(4).write.mode("overwrite").parquet(save_path) # 8 writers ca mets un warn 95% donc on va a 4 (c'est pas coalesce donc je vais forcement a 4)

