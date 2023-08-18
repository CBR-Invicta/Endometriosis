from sqlalchemy import create_engine
import dotenv
import psycopg2
import os
import sys
import pandas as pd
import numpy as np
import seaborn as sns
import base64
import datetime
from fuzzywuzzy import fuzz
import difflib
import matplotlib.pyplot as plt
from typing import List
import re
import json
from dotenv import load_dotenv
from sklearn.manifold import TSNE

from read_data import get_query, get_txt
from cleaning import clean_data, unidecode_list, lemma_list
from str_matching import match_words


class t_SNE:

    def __init__(self, data):
        """Klasa z modelami t_SNE



        Parameters
        ----------
        data : pd.Dataframe
            tabela z danymi źródłowymi

        Methods:
        ----------
            tSNE_fit_transform(**kwargs):
                funkcja, która przy użyciu algorytmu tSNE redukuje wymiar zbioru danych
            tSNE_plot(**kwargs):
                tworzy wykres wykorzystujący wartości z funkcie tSNE_fit_transform w porównaniu do wybranej kolumny
        """

        self.data: pd.DataFrame = data

    def tSNE_fit_transform(self,
                           data: pd.DataFrame,
                           n_components: int = 2,
                           init: str = "pca",
                           perplexity: float = 30.0,
                           metric: str = "euclidean",
                           learning_rate: float = 200.0,
                           method: str = "barnes_hut",
                           n_iter: int = 1000,
                           verbose: int = 1):
        """tSNE_fit jest to funkcja, która przy użyciu algorytmu tSNE redukuje wymiar zbioru danych (dla korelacji nieliniowej)

        Parameters
        ----------
        data : pd.DataFrame
            Zbiór danych do przeprocesowania
        n_components : int, optional
            Wymiar przestrzeni, by default 2
        init : str, optional
            Inicjalizacja, możliwe opcje: "random", "pca" by default "pca"
        perplexity : float, optional
            Liczba najblizszych sąsiadów, większe zbiory wymagają większej wartości z zakresu od 5 do 50, by default 30.0
        metric : str, optional
            Metryka używana podczas obliczania odległości między punktami, by default "euclidean"
        learning_rate : float, optional
            _description_, by default 200.0
        method : str, optional
            _description_, by default "barnes_hut"
        n_iter : int, optional
            Maksymalna liczba iteracji bez postępu przed przerwaniem optymalizacji , by default 1000
        verbose : int, optional
            _description_, by default 1

        Returns
        -------
        np.array 
            Zredukowany wymiar zbioru danych
        """

        print("Fitting dataset with tSNE ...")

        tsne = TSNE(
            n_components=n_components,
            init=init,
            perplexity=perplexity,
            metric=metric,
            learning_rate=learning_rate,
            method=method,
            n_iter=n_iter,
            verbose=verbose,
        )

        tsne_features = tsne.fit_transform(self.data)

        print(f"Shape of dataset before fitting: {self.data.shape}")
        print(f"Shape of dataset after fitting: {tsne_features.shape}")

        return tsne_features

    def tSNE_plot(self, data: pd.DataFrame, tsne_features: np.array, column_name: str):
        """tSNE_plot tworzy wykres wykorzystujący wartości z funkcie tSNE_fit_transform w porównaniu do wybranej kolumny

        Parameters
        ----------
        data : pd.DataFrame
            Zbiór danych wejściowych
        tsne_features : np.array
            Cechy uzyskane z funkcji fit_transform
        column_name : str
            Nazwa kolumny, którą chcemy zaprezentować na wykresie

        Returns
        -------
        plt
            Wykres tSNE 
        """

        self.data['x'] = tsne_features[:, 0]
        self.data['y'] = tsne_features[:, 1]

        sns.scatterplot(x='x', y='y', hue=column_name, data=data)

        return plt.show()
