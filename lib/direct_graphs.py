from sqlalchemy import create_engine
import dotenv
import psycopg2
import os
import sys
import pandas as pd
import numpy as np
import base64
import json
import datetime
from fuzzywuzzy import fuzz
import difflib
import matplotlib.pyplot as plt
from typing import List
import re
from collections import Counter
from itertools import chain
from dotenv import load_dotenv


from read_data import get_query, get_txt
from cleaning import clean_data, unidecode_list, lemma_list
from str_matching import match_words
import networkx as nx
import scipy
import matplotlib as mpl
import netgraph


def direct_prep_data(
    data: pd.DataFrame,
    value_count: int = 3000,
    visit_id: str = "patient_id_wizyta",
    start_date_col: str = "visit_date",
    source: str = "profil_analiza_short",
) -> pd.DataFrame:
    """direct_prep_data przygotowuje zbiór danych wejściowych do funkcji direct_graph (dodanie kolumn z badaniami o jeden okres wcześniej)

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    value_count : int, optional
        Dobór badań, których minimalna ilość w zbiorze wynosi, by default 2000
    visit_id : str, optional
        użytkownik, by default "patient_id_wizyta"
    start_date_col : str, optional
        data na podstawie ,której będzie stworzony kierunek krawędzi, by default "visit_date"
    source : str, optional
        węzły w grafie, by default "profil_analiza_short"

    Returns
    -------
    pd.DataFrame
        Zbiór danych przygotowany do direct_graph
    """

    df = data.loc[
        np.isin(
            data.profil_analiza_short,
            data.profil_analiza_short.value_counts().index[
                data.profil_analiza_short.value_counts() > value_count
            ],
        )
    ].copy()
    df = df.sort_values(by=[visit_id, start_date_col])

    df[f"prev_proc-{visit_id}"] = df[visit_id].shift(1)
    df[f"prev_proc-{source}"] = df[source].shift(1)
    df.loc[df[visit_id] !=
           df[f"prev_proc-{visit_id}"], f"prev_proc-{source}"] = np.NaN
    df.loc[df[source] ==
           df[f"prev_proc-{source}"], f"prev_proc-{source}"] = "Koniec"
    df[f"prev_proc-{source}"] = df[f"prev_proc-{source}"].fillna("Koniec")

    start = df.copy()
    start[source] = "Start"
    start.drop(
        start[start[f"prev_proc-{source}"] == "Koniec"].index, inplace=True)

    result = pd.concat([df, start])

    result["weight"] = result.groupby([visit_id, source])[
        source].transform("size")

    return result


def direct_graph(
    data: pd.DataFrame,
    source: str = "profil_analiza_short",
    target: str = "prev_proc-profil_analiza_short",
    edge_attr: str = "weight",
    k: int = 1.1,
    scale: int = 5,
    seed: int = 2,
    arrowsize: int = 20,
    alpha: int = 0.8,
):
    """direct_graph jest to metoda, która tworzy direct graph na podstawie danych wejściowych. Metoda ma na celu przedstawienie 'drogi' badań, które są wykonywane po sobie. 
    Rozmiar węzłów zależna jest od ilości krawędzi, które z niego wychodzą. Grubość krawędzi zależna jest od kolumny przypisanej jako edge_attr. Punkt Start jest ustalony jako maksymalne współrzędne w zbiorze. 

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    source : str, optional
        kolumna dla której chcemy oznaczyc drogę, by default "profil_analiza_short"
    target : str, optional
        kolumna z następnym krokiem w grafie, by default "prev_proc-profil_analiza_short"
    edge_attr : str, optional
        kolumna, która określa wagę (grubość) krawędzi, by default "weight"
    k : int, optional
        odległość węzłów od siebie, by default 1.1
    scale : int, optional
        skala grafu, by default 5
    seed : int, optional
        seed dla reproduktywności, by default 2
    arrowsize : int, optional
        rozmiar strzałki w krawędziach, by default 20
    alpha : int, optional
        przezroczystość, by default 0.8

    Returns
    -------
    plt
        Funkcja zwraca direct graph
    """
    G = nx.from_pandas_edgelist(
        data,
        source=source,
        target=target,
        create_using=nx.DiGraph(),
        edge_attr=edge_attr,
    )

    plt.figure(figsize=(20, 20))

    # node size
    node_size = []
    for node in G:
        if node == "Start":
            node_size.append(G.out_degree()["Start"])
        if node == "Koniec":
            node_size.append(G.in_degree()["Koniec"])
        else:
            for val in dict(G.degree()).values():
                node_size.append(val)
    # edge width
    edges = G.edges()
    weights = [G[u][v][edge_attr] for u, v in edges]

    # positions of nodes (with fixed Start)
    pos = nx.spring_layout(G, k=k, scale=scale, seed=seed)
    color_map = []

    arr_1 = []
    arr_2 = []

    for p, array in pos.items():
        arr_1.append(array[0])
        arr_2.append(array[1])

    pos["Start"] = np.array(list([max(arr_1), max(arr_2)]))

    for node in G:
        if node == "Start" or node == "Koniec":
            color_map.append("firebrick")
        else:
            color_map.append("goldenrod")

    nx.draw(
        G,
        pos,
        with_labels=True,
        node_color=color_map,
        arrowsize=arrowsize,
        alpha=alpha,
        edge_color="grey",
        node_size=[v * 100 for v in dict(G.degree()).values()],
        width=[v / 10 for v in weights],
    )
    plt.show()

    return nx.from_pandas_edgelist(
        data,
        source=source,
        target=target,
        create_using=nx.DiGraph(),
        edge_attr=edge_attr,
    )


def plot_graph(
    data: pd.DataFrame,
    source: str = "profil_analiza_short",
    target: str = "prev_proc-profil_analiza_short",
    edge_attr: str = "weight",
    k: int = 1.1,
    scale: int = 5,
    seed: int = 2,
    arrowsize: int = 20,
    alpha: int = 0.8,
    planar=False
):
    """direct_graph jest to metoda, która tworzy direct graph na podstawie danych wejściowych. Metoda ma na celu przedstawienie 'drogi' badań, które są wykonywane po sobie. 
    Rozmiar węzłów zależna jest od ilości krawędzi, które z niego wychodzą. Grubość krawędzi zależna jest od kolumny przypisanej jako edge_attr. Punkt Start jest ustalony jako maksymalne współrzędne w zbiorze. 

    Parameters
    ----------
    data : pd.DataFrame
        Zbiór danych wejściowych
    source : str, optional
        kolumna dla której chcemy oznaczyc drogę, by default "profil_analiza_short"
    target : str, optional
        kolumna z następnym krokiem w grafie, by default "prev_proc-profil_analiza_short"
    edge_attr : str, optional
        kolumna, która określa wagę (grubość) krawędzi, by default "weight"
    k : int, optional
        odległość węzłów od siebie, by default 1.1
    scale : int, optional
        skala grafu, by default 5
    seed : int, optional
        seed dla reproduktywności, by default 2
    arrowsize : int, optional
        rozmiar strzałki w krawędziach, by default 20
    alpha : int, optional
        przezroczystość, by default 0.8

    Returns
    -------
    plt
        Funkcja zwraca direct graph
    """
    G = nx.from_pandas_edgelist(
        data,
        source=source,
        target=target,
        # create_using=nx.DiGraph(),
        edge_attr=edge_attr,
    )

    plt.figure(figsize=(15, 15))

    # positions of nodes (with fixed Start)
    if planar:
        pos = nx.planar_layout(G)
    else:
        pos = nx.spring_layout(G)
    nx.draw(
        G,
        pos,
        with_labels=True,
        arrowsize=arrowsize,
        alpha=alpha,
        edge_color="grey",
        font_size=12
    )
    nx.draw_networkx_edge_labels(G, pos, edge_labels=dict(
        zip(list(data[[source, target]].itertuples(index=False)), data[edge_attr])), font_size=12)

    # netgraph.EditableGraph(G, node_positions=pos)
    plt.show()

    # return nx.from_pandas_edgelist(
    #     data,
    #     source=source,
    #     target=target,
    #     create_using=nx.DiGraph(),
    #     edge_attr=edge_attr,
    # )


class Direct_Graph:

    def __init__(self, data=None):

        self.data: pd.DataFrame = data
        self.processed_data: pd.DataFrame = None

    def load_data(self, sql: str = "visit_p1_lekarze_zlecenie_warehouse", domain: str = "DOMAIN_data_warehouse",):
        """load_data pobiera dane z data warehouse. Upewnij się, że w zmiennych środowiskowych są zdefiniowane wartości USER_data_warehouse, PASSWORD_data_warehouse, DOMAIN_data_warehouse."""
        print("Loading data...")
        self.data = get_query(
            sql,
            user="USER_data_warehouse",
            password="PASSWORD_data_warehouse",
            domain=domain,
        )
        #self.data = self.data.drop_duplicates()
        print("Done.")

    def process_data(self, **kwargs):

        print(f"Processing data...")

        self.processed_data = direct_prep_data(self.data, **kwargs)

        print(f"Done")

    def draw_graph(self, **kwargs):

        print(f"Creating direct graph...")
        if isinstance(self.processed_data, pd.DataFrame):
            self.graph = direct_graph(self.processed_data, **kwargs)
        else:
            self.graph = plot_graph(self.data, **kwargs)
