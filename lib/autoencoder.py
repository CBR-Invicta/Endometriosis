import math
import pandas as pd
import tensorflow as tf
import matplotlib.pyplot as plt
from tensorflow.keras import Model
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, Dropout, LeakyReLU
from tensorflow.keras.losses import MeanSquaredLogarithmicError
from tensorflow.keras.optimizers import Adam
import numpy as np

from typing import Tuple


class AutoEncoders(Model):

    def __init__(
            self,
            output_units,
            BOTTLENECK_SIZE=4):

        super().__init__()
        self.encoder = Sequential(
            [
                Dense(
                    round(output_units / 2),
                    activation=tf.keras.layers.LeakyReLU(alpha=0.02),
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    # bias_regularizer=tf.keras.regularizers.L1L2(
                    #     l1=0.01, l2=0.01)
                ),
                Dense(
                    round(output_units / 4),
                    activation=tf.keras.layers.LeakyReLU(alpha=0.02),
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    # bias_regularizer=tf.keras.regularizers.L1L2(
                    #     l1=0.01, l2=0.01)
                ),
                Dense(
                    BOTTLENECK_SIZE,
                    activation='linear',
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    use_bias=False
                ),
            ]
        )

        self.decoder = Sequential(
            [
                Dense(
                    round(output_units / 4),
                    activation=tf.keras.layers.LeakyReLU(alpha=0.02),
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    # bias_regularizer=tf.keras.regularizers.L1L2(
                    #     l1=0.01, l2=0.01)
                ),
                Dense(
                    round(output_units / 2),
                    activation=tf.keras.layers.LeakyReLU(alpha=0.02),
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    # bias_regularizer=tf.keras.regularizers.L1L2(
                    #     l1=0.01, l2=0.01)
                ),
                Dense(
                    output_units,
                    activation="linear",
                    kernel_initializer='glorot_uniform',
                    # kernel_regularizer=tf.keras.regularizers.l1(0.01),
                    # activity_regularizer=tf.keras.regularizers.l2(0.01),
                    # bias_regularizer=tf.keras.regularizers.L1L2(
                    #     l1=0.01, l2=0.01),
                    use_bias=False
                )
            ]
        )

    def fit_autoencoder(self, dataset):
        early_stop = tf.keras.callbacks.EarlyStopping(
            monitor="val_loss",
            min_delta=0.01,
            patience=50,
            verbose=0,
            mode="min",
            restore_best_weights=True,
        )
        self.compile(
            loss='mse',
            metrics=['mse'],
            optimizer=Adam(learning_rate=tf.keras.optimizers.schedules.ExponentialDecay(
                initial_learning_rate=0.05, decay_steps=100, decay_rate=0.96, staircase=False, name='decay_lr'
            ))
        )
        self.fit(
            dataset,
            dataset,
            epochs=1000,
            callbacks=[early_stop],
            batch_size=int(np.round(dataset.shape[0]/4)),
            validation_split=0.2,
            verbose=1
        )

    def call(self, inputs):

        encoded = self.encoder(inputs)
        decoded = self.decoder(encoded)
        return decoded

    def encode(self, inputs):
        encoder_output = self.encoder.predict(inputs)
        return encoder_output

    def predict_autoencoder(self,
                            data_sample: np.ndarray):
        autoencoder_output = self.decoder(
            self.encoder.predict(data_sample).astype(np.float64))
        return list(np.ravel(np.argsort(autoencoder_output*(1-data_sample)))[::-1])

    def evaluate_missing_element_model(
            self,
            test_df: pd.DataFrame,
            test_size: int,
            n_predictions: int = 3) -> Tuple[float, list]:
        """ Evaluates binary features recommendation model by randomly hiding(setting to 0) active features(value = 1) and
        assesing the prediction result by a custom score and performance histogram.

        Score:
            the score used for evaluation is calculated by counting the number of times that one of 'n_predictions' most likely
            predictions matched the hidden feature aka ground truth.

        Performance histogram:
            the second returned value can be plotted as a histogram of prediction indices matching the ground truth

        Args:
            test_df (pd.DataFrame): input dataframe from which test samples are gonna be randomly chosen
            model_predict_func (function): model prediction methods - function of 1 argument(numpy.ndarray) returning sorted
                                        list of recommended indices in order of their probability - 1st element-most likely
            test_size (int): number of test samples used to evaluate the model
            n_predictions (int, optional): number of elements to be considered while calculating model's score. Defaults to 3.

        Returns:
            Tuple[float, list]: Tuple of calculated score and list of prediction indices that matched ground truth(e.g. 
                                0 when the most likely prediction was correct,
                                1 when the 2nd most likely prediction was correct etc.)
        """
        test_results_binary = []
        test_results_index = []
        for _ in range(test_size):
            sample = test_df.sample().to_numpy()
            items_indices = np.argwhere(sample)

            random_idx = np.random.randint(0, len(items_indices))
            masked_item_idx = items_indices[random_idx, 1]
            sample[0, masked_item_idx] = 0

            predicted_idx = self.predict_autoencoder(sample)

            test_results_binary.append(
                masked_item_idx in predicted_idx[:n_predictions])
            test_results_index.append(np.argwhere(
                predicted_idx == masked_item_idx)[0][0])

        return sum(test_results_binary)/len(test_results_binary), test_results_index


'''Example
auto_encoder = AutoEncoders(len(dataset.columns))

auto_encoder.compile(
    loss='binary_crossentropy',
    metrics=['binary_crossentropy'],
    optimizer=Adam(learning_rate=0.001)
)

history = auto_encoder.fit(
    dataset,
    dataset,
    epochs=100,
    batch_size=128,
    validation_data=(dataset, dataset),
)
rcParams['figure.figsize'] = 12, 12

score, indices = auto_encoder.evaluate_missing_element_model(
    dataset,  test_size=5000, n_predictions=3)

plt.title(f"Score: {score}")
sns.histplot(indices, binwidth=1)
sample = [1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]
auto_encoder.decoder(auto_encoder.encoder.predict(np.expand_dims(
    sample, axis=0).astype(np.float32))) * (1-np.array(sample))
'''
