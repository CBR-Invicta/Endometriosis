import pandas as pd
import numpy as np
import seaborn as sns
from typing import Any, Dict, List
import matplotlib.pyplot as plt

from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from scipy.stats import iqr
from utils import impute_data, DataSerieEDA
import pickle

settings = {
    "threshold": 0.60,
    "all_columns": True,
    "visualize": True,
    "y_column": None,
    "lof_n_neighbors": 20,
}


def visualize_2d_if(
    if_score: List[float], x: pd.Series, y: pd.Series, model: IsolationForest
):
    """visualize_2d_if tworzy wykresy counterplot dla modeli Isolation Forest w przypadku analizy 2 zmiennych.

    Parameters
    ----------
    if_score : List[float]
        Wynik algorytmu IF
    x : pd.Series
        Wartości zmiennej niezależnej
    y : pd.Series
        Wartości zmiennej zależnej
    model : IsolationForest
        Wytrenowany model
    """
    X_JITTER_STRENGTH = 0.1
    Y_JITTER_STRENGTH = 0.1
    fig, axs = plt.subplots(ncols=2)

    axs[0].scatter(range(len(if_score)), if_score, s=10)

    xx, yy = np.meshgrid(
        np.linspace(
            x.min() - 5 * X_JITTER_STRENGTH,
            x.max() + 5 * X_JITTER_STRENGTH,
            200,
        ),
        np.linspace(
            y.min() - 5 * Y_JITTER_STRENGTH,
            y.max() + 5 * Y_JITTER_STRENGTH,
            200,
        ),
    )
    Z = model.decision_function(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)

    axs[1].contourf(xx, yy, Z, cmap=plt.cm.Blues_r)

    x_rdn_jitter = np.random.uniform(-X_JITTER_STRENGTH,
                                     X_JITTER_STRENGTH, len(x))
    y_rdn_jitter = np.random.uniform(-Y_JITTER_STRENGTH,
                                     Y_JITTER_STRENGTH, len(y))

    b1 = axs[1].scatter(
        x + x_rdn_jitter,
        y + y_rdn_jitter,
        c="gray",
        s=15,
        edgecolor="k",
        alpha=0.5,
    )

    axs[1].axis("tight")
    axs[0].set_xlabel("n observation")
    axs[0].set_ylabel("anomaly score")
    axs[1].set_xlabel(x.name)
    axs[1].set_ylabel(y.name)
    fig.set_size_inches(18, 8)
    plt.show()


class detect_outliers:
    """Klasa do usuwania wartości skrajnych w zbiorze danych przed modelowaniem.
    Obsługuje problemy regresji, klasyfikacji binarnej, multiclass i multilabel classification.

    Attributes:
    ----------

        data: pd.DataFrame
            Zbiór danych wejściowych
        filtered_data: pd.DataFrame = None
            Odfiltrowany zbiór danych (Bez wartości skrajnych)
        outliers_if: pd.Series = None
            Czy obserwacja została uznana za skrajną przez model IF? (0-1)
        outliers_lof: pd.Series = None
            Czy obserwacja została uznana za skrajną przez model LOF? (0-1)
        outliers_iqr: pd.Series = None
            Czy obserwacja została uznana za skrajną przez metodę IQR? (0-1)
        outliers: pd.Series = None
            Czy obserwacja została uznana za skrajną przez model IF? (0-1)
        settings: Dict[str, Any] = settings
            Ustawienia klasy zawierające:
                threshold
                    Powyżej jakiego punktu odcięcia obserwacja jest uważana za skrajną w IF (default 0.65)
                all_columns
                    Czy algorytm IF powinien wyszukiwać obserwacji skrajnych na całym zbiorze (True) czy iteracyjnie po kombinacjach zmiennych niezależnych oraz zmiennej zależnej (False) (default True)
                visualize
                    Czy wizualizować wynik IF? (default True)
                y_column
                    Lista nazw kolumn zmiennych zależnych (default None)
                lof_n_neighbors
                    Liczba sąsiadów wykorzystywana przez algorytm LOF (default 50)

    Methods:
    ----------
        detect_outliers_if():
            opis
        detect_outliers_lof():
            opis
        detect_outliers_iqr():
            opis
        remove_outliers(methods: List[str] = ["if", "lof", "iqr"], how_many_positives: int = 1):
            opis

    """

    def __init__(self, data: DataSerieEDA = None):
        self.data_serie: DataSerieEDA = data
        self.data: pd.DataFrame = data.input_df[data.columns]
        self.imputed_data: pd.DataFrame = impute_data(
            data.input_df[data.columns])
        self.filtered_data: DataSerieEDA = None
        self.outliers_if: pd.Series = None
        self.outliers_lof: pd.Series = None
        self.outliers_iqr: pd.Series = None
        self.outliers: pd.Series = None
        self.settings: Dict[str, Any] = settings

    def detect_outliers_if(self):
        """detect_outliers_if oznacza obserwacje skrajne przy użyciu algorytmu IF

        Na zachowanie algorytmu wpływają wartości self.settings (threshold, all_columns, visualize oraz y_column).

        Result:
        ----------
            self.outliers_if: pd.Series
                Czy obserwacja została uznana za skrajną przez model IF? (0-1)
        """
        threshold = self.settings["threshold"]
        all_columns = self.settings["all_columns"]
        visualize = self.settings["visualize"]
        y_column = self.settings["y_column"]
        if all_columns:
            print(
                f"Method: Isolation Forest, all columns at once. Threshold : {threshold}"
            )

            clf = IsolationForest(
                random_state=42,
                max_features=5,
                n_estimators=100,
                verbose=1,
                contamination=0.05,
            ).fit(self.imputed_data.values)

            score_if = -1 * clf.score_samples(self.imputed_data.values)
            is_outlier = (score_if > threshold) * 1
            if visualize:
                plt.scatter(
                    range(len(self.imputed_data)),
                    score_if,
                )
                plt.plot([0, len(self.imputed_data)], [
                         threshold, threshold], color="r")
                plt.show()
            print(
                f"Removed {(is_outlier != 0).sum()} observations - {(is_outlier != 0).sum()/len(self.imputed_data)*100}%"
            )
            self.outliers_if = pd.Series(is_outlier, index=self.data.index)
        else:
            # Outliers filtering - feature pairs sequentially
            self.imputed_data["is_outlier"] = np.zeros(len(self.imputed_data))

            if y_column is None:
                raise ValueError(
                    f'Wartość kolumny zależnej nie jest ustalona w settings. Zmień wartość settings["y_column"]'
                )

            if isinstance(y_column, list):
                visualize = False
            else:
                y_column = [y_column]

            for column_name in self.imputed_data.drop(
                columns=y_column + ["is_outlier"]
            ):

                OUTLIER_COLS = [column_name] + y_column

                clf = IsolationForest(
                    random_state=42,
                    max_features=2,
                    n_estimators=256,
                    verbose=0,
                    contamination=0.05,
                    n_jobs=8,
                ).fit(
                    self.imputed_data[self.imputed_data["is_outlier"] == 0][
                        OUTLIER_COLS
                    ].values
                )
                if_score = (
                    clf.score_samples(
                        self.imputed_data[OUTLIER_COLS].values) * -1
                )

                liers_count_before = (
                    self.imputed_data["is_outlier"] != 0).sum()
                self.imputed_data["is_outlier"] += (if_score > threshold) * 1
                liers_count_after = (
                    self.imputed_data["is_outlier"] != 0).sum()
                if visualize is True:
                    visualize_2d_if(
                        if_score,
                        self.imputed_data[column_name],
                        self.imputed_data[y_column[0]],
                        clf,
                    )
                print(f"Method IF, analyzing {column_name} feature...")
                print(
                    f"Found {liers_count_after - liers_count_before} outliers")

            print(
                f"Removed {(self.imputed_data['is_outlier'] != 0).sum()} observations - {(self.imputed_data['is_outlier'] != 0).sum()/len(self.imputed_data)*100}%"
            )
            self.outliers_if = (self.imputed_data["is_outlier"] > 0) * 1
            self.imputed_data.drop(columns="is_outlier", inplace=True)

    def detect_outliers_lof(self) -> pd.Series:
        """detect_outliers_lof oznacza obserwacje skrajne przy użyciu algorytmu LOF

        Na zachowanie algorytmu wpływają wartości self.settings (threshold, all_columns, visualize oraz y_column).

        Result:
        ----------
            self.outliers_lof: pd.Series
                Czy obserwacja została uznana za skrajną przez model LOF? (0-1)
        """
        lof = LocalOutlierFactor(
            n_neighbors=self.settings["lof_n_neighbors"],
            algorithm="auto",
            leaf_size=30,
            metric="minkowski",
            p=2,
            metric_params=None,
            contamination="auto",
            novelty=False,
            n_jobs=None,
        )
        outliers = lof.fit_predict(self.imputed_data)
        pd.Series(lof.negative_outlier_factor_).hist(bins=30)
        outliers = (outliers == -1) * 1
        self.outliers_lof = pd.Series(outliers, index=self.data.index)
        print(f"Method: LOF, found {sum(outliers)} outliers")

    def detect_outliers_iqr(self) -> pd.Series:
        """detect_outliers_iqr oznacza obserwacje skrajne przy użyciu metody IQR

        Na zachowanie algorytmu wpływają wartości self.settings (threshold, all_columns, visualize oraz y_column).

        Result:
        ----------
            self.outliers_iqr: pd.Series
                Czy obserwacja została uznana za skrajną przez metodę IQR? (0-1)
        """
        full_outlier_list = pd.Series(
            np.zeros(self.data.shape[0]), index=self.data.index
        )
        for column_name in self.data:
            if not self.data[column_name].unique().__len__() <5:
                print(f"Method: IQR, analyzing {column_name} feature...")
                maximum = self.data[column_name].quantile(0.75) + 2.5 * iqr(
                    self.data[column_name], nan_policy="omit"
                )
                minimum = self.data[column_name].quantile(0.25) - 2.5 * iqr(
                    self.data[column_name], nan_policy="omit"
                )
                outliers = (
                    (self.data[column_name] < minimum) | (
                        self.data[column_name] > maximum)
                ) * 1
                print(f"Found {sum(outliers)} outliers")
                full_outlier_list = full_outlier_list + outliers
        full_outlier_list = (full_outlier_list > 0) * 1
        self.outliers_iqr = full_outlier_list

    def remove_outliers(
        self, methods: List[str] = ["if", "lof", "iqr"], how_many_positives: int = 1
    ):
        """remove_outliers usuwa wartości skrajne przy pomocy wybranych metod

        _extended_summary_

        Parameters
        ----------
        methods : List[str], optional
            lista metod, które zostaną użyte do detekcji wartości skrajnych, by default ['if', 'lof','iqr']
        how_many_positives : int, optional
            ile metod musi oznaczyć wartość jako skrajną, aby zostala usunięta ze zbioru, by default 1
        """
        for method in methods:
            getattr(self, "detect_outliers_" + method)()

        try:
            outliers = pd.DataFrame(
                [getattr(self, "outliers_" + method) for method in methods]
            )
        except:
            print('No outliers found')
            self.filtered_data = DataSerieEDA(
                input_df=self.data,
                y_columns=self.data_serie.y_columns,
                columns=self.data_serie.columns)
            return
        self.outliers = (outliers.sum(axis=0) >= how_many_positives) * 1
        print(
            f"Summary: with {len(methods)} methods({methods}), {sum(self.outliers)} outliers were found."
        )
        self.filtered_data = DataSerieEDA(
            input_df=self.data.loc[self.outliers == 0],
            y_columns=self.data_serie.y_columns,
            columns=self.data_serie.columns)

    def save_results(self):
        with open('detect_outliers_results.pickle', 'wb') as handle:
            pickle.dump(vars(self), handle, protocol=pickle.HIGHEST_PROTOCOL)
