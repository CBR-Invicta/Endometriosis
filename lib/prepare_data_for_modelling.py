import pandas as pd
import numpy as np
import seaborn as sns
from typing import Any, Dict, List
import matplotlib.pyplot as plt
import os

from detect_outliers import detect_outliers
from feature_selection import feature_selection
from balanse_data import balanse_data
from premodeling_rules import Rules
from utils import DataSerieEDA
import pickle

from utils import split_train_test

data_path = os.getenv("data_cbr")


class EDA:
    def __init__(
        self,
        data_serie: DataSerieEDA,
        premodeling_rules: Rules = None,
        outliers_detection: detect_outliers = None,
        feature_selection: feature_selection = None,
        balansing_data: balanse_data = None,
    ):
        self.data_serie = data_serie
        self.premodeling_rules = premodeling_rules
        self.outliers_detection = outliers_detection
        self.feature_selection = feature_selection
        self.balansing_data = balansing_data

    def remove_columns_hardrules(self, **kwargs):
        self.premodeling_rules = Rules(self.data_serie)
        self.premodeling_rules.process_rules(**kwargs)

    def detect_outliers(self, methods: List[str] = ["if", "lof", "iqr"], how_many_positives: int = 1):
        assert self.premodeling_rules is not None, "Run remove_columns_hardrules before detect_outliers."

        self.outliers_detection = detect_outliers(
            self.premodeling_rules.filtered_data
        )
        self.outliers_detection.remove_outliers(
            methods=methods,
            how_many_positives=how_many_positives
        )

    def select_features(self, methods: List[str] = ["recursively", "boruta_shap"], how_many_positives: int = 1):
        assert self.outliers_detection is not None, "Run detect_outliers before select_features."
        self.feature_selection = feature_selection(
            data=self.outliers_detection.filtered_data)
        self.feature_selection.select_features(
            methods=methods, how_many_positives=how_many_positives)

    def save_results(self, folder_name: str):
        cwd = os.getcwd()
        os.chdir(os.getenv('data_cbr'))
        try:
            os.mkdir(folder_name)
        except:
            print('Adding files to already existing folder')

        os.chdir(folder_name)
        self.premodeling_rules.save_results()
        self.outliers_detection.save_results()
        self.feature_selection.save_results()
        os.chdir(cwd)

    def pipeline(self,
                 folder_name: str,
                 outliers_methods: List[str] = ["if", "lof", "iqr"],
                 feature_selection_methods: List[str] = [
                     "recursively", "boruta_shap"],
                 **kwargs):
        self.remove_columns_hardrules(**kwargs)
        self.detect_outliers(methods=outliers_methods)
        self.select_features(methods=feature_selection_methods)
        self.save_results(folder_name)
