import os
import tensorflow as tf
from tensorflow import keras
from dataset_functions import get_tf_dataset

def train_model(path, save_path,model_name,epochs=100):
    dataset, detected_shape = get_tf_dataset(path)

    model = keras.Sequential([
        keras.layers.Input(shape=(None, None, detected_shape[-1])),
        keras.layers.Conv2D(32, (3, 3), activation="relu"),
        keras.layers.MaxPooling2D((2, 2)),
        keras.layers.Conv2D(64, (3, 3), activation="relu"),
        keras.layers.MaxPooling2D((2, 2)),
        keras.layers.GlobalAveragePooling2D(),  # remplace Flatten()
        keras.layers.Dense(64, activation="relu"),
        keras.layers.Dropout(0.1),
        keras.layers.Dense(1, activation="sigmoid")
    ], name="model_mask")

    model.compile(
        optimizer="adam",
        loss="binary_crossentropy",
        metrics=["accuracy"]
    )

    dataset = dataset.shuffle(1000).batch(64).prefetch(tf.data.AUTOTUNE) 
    # 1000 c'est le buffer / ca shuffle tout le dataset puis fait des batch
    # Prefetch c'est pour pour que le modele fasse des batchs pendant l'entrainement 

    model.fit(
        dataset,
        epochs=epochs,
    )

    os.makedirs(save_path, exist_ok=True)
    model.save(  os.path.join(save_path, model_name + ".keras")) # Pour recalculer l'accuracy 
    model.export(os.path.join(save_path, model_name + ".onnx"),format="onnx") # Pour scala

    print(f"Model saved to {save_path}")

    return model
