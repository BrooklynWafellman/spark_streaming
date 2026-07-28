import tensorflow as tf
import polars as pl
import numpy as np

def get_tf_dataset(parquet_path):
    df = pl.read_parquet(f"{parquet_path}/*.parquet")

    detected_shape = df["processed_shape"][0]
    n_channels = detected_shape[-1]

    def generator():
        for row in df.iter_rows(named=True):
            shape = tuple(row["processed_shape"])
            img = np.frombuffer(row["processed"], dtype=np.uint8).reshape(shape).astype(np.float32) / 255.0
            label = int(row["label_index"])
            yield img, label

    dataset = tf.data.Dataset.from_generator(
        generator,
        output_signature = (
            tf.TensorSpec(shape=(None, None, n_channels), dtype=tf.float32),
            tf.TensorSpec(shape=(), dtype=tf.int32)
        )
    )

    return dataset, detected_shape