import numpy as np
import tensorflow as tf
from dataset_functions import get_tf_dataset

def evaluate(val_path: str, model):
    dataset, _ = get_tf_dataset(val_path)

    dataset = dataset.batch(64).prefetch(tf.data.AUTOTUNE) # pas besoin de shuffle pour un eval 

    y_true = []
    y_pred = []

    for imgs, labels in dataset:
        preds = model.predict(imgs, verbose=0).flatten()
        y_pred.extend((preds >= 0.5).astype(int).tolist())
        y_true.extend(labels.numpy().tolist())

    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    # --- Model accuracy: mask or not ---
    acc = np.mean(y_pred == y_true)
    print(f"Model accuracy (w/ mask vs w/o mask): {acc:.4f}")