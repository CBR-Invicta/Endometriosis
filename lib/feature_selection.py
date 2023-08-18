from urllib import response
import pandas as pd
import numpy as np
import json
import pickle
from IPython.display import display

from sklearn.feature_selection import RFECV
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import Ridge, RidgeClassifier, Lasso
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from typing import List, Any, Dict, Tuple, Union
import inspect
from traitlets import Bool
from xgboost import XGBClassifier, XGBRegressor
from BorutaShap import BorutaShap

import skcriteria as skc
from skcriteria.madm import simple, moora
from skcriteria.preprocessing import invert_objectives

from utils import split_train_test, impute_data, DataSerieEDA, check_objective

# Suggested models for classification tasks: RidgeClassifier, DecisionTreeClassifier, RandomForestClassifier
# Suggested models for regression tasks: Ridge, Lasso, DecisionTreeRegressor, RandomForestRegressor
models_settings = {
    "Ridge": {"alpha": 0.8, "fit_intercept": True},
    "RidgeClassifier": {"alpha": 0.8, "fit_intercept": True},
    "Lasso": {"alpha": 0.8, "fit_intercept": True},
    "DecisionTreeClassifier": {
        "max_depth": 10,
        "min_samples_split": 10,
        "max_features": "log2",
        "random_state": 42,
        "max_leaf_nodes": 32,
    },
    "DecisionTreeRegressor": {
        "max_depth": 10,
        "min_samples_split": 10,
        "max_features": "log2",
        "random_state": 42,
        "max_leaf_nodes": 32,
    },
    "RandomForestClassifier": {
        "max_depth": 5,
        "min_samples_split": 10,
        "max_features": "log2",
        "random_state": 42,
        "max_leaf_nodes": 16,
    },
    "RandomForestRegressor": {
        "max_depth": 5,
        "min_samples_split": 10,
        "max_features": "log2",
        "random_state": 42,
        "max_leaf_nodes": 16,
    },
}


def select_recursively(
    data: pd.DataFrame,
    y_columns: str,
    models: List[Any] = None,
    objective: str = "auto",
    cv: int = 5,
) -> pd.DataFrame:
    """select_recursively _summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        _description_
    y : str
        _description_
    models : List[Any]
        _description_
    objective : str, optional
        _description_, by default 'auto'
    cv : int, optional
        _description_, by default 5

    Returns
    -------
    pd.DataFrame
        _description_

    Raises
    ------
    TypeError
        _description_
    """
    X = data.drop(columns=y_columns).copy()
    y = data[y_columns].copy()

    nans = pd.isna(X).sum().sum() + pd.isna(y).sum().sum()

    if type(objective) != str:
        raise TypeError(
            'objective must be a string: "auto", "regression" or "classification"'
        )
    if objective == "auto":
        if (y.unique().__len__() == 2) | pd.api.types.is_string_dtype(y):
            objective = "classification"
            if models is None:
                models = [
                    # "RidgeClassifier",
                    # "DecisionTreeClassifier",
                    "RandomForestClassifier",
                ]
        else:
            objective = "regression"
            if models is None:
                models = [
                    "Ridge",
                    "Lasso",
                    # "DecisionTreeRegressor",
                    "RandomForestRegressor",
                ]
    response = {}
    for model in models:
        if (nans > 0) & (model in ["Ridge", "RidgeClassifier", "Lasso"]):
            next
        else:
            estimator = globals()[model](**models_settings[model])
            selector = RFECV(estimator, cv=cv, verbose=1)
            selector = selector.fit(X, y)
            response[model] = {
                "support": dict(zip(X.columns.to_list(), selector.support_)),
                "ranking": dict(zip(X.columns.to_list(), selector.ranking_)),
            }
    return response


def select_rf(
        data: pd.DataFrame,
        y_columns: str,
        objective: str = "auto") -> pd.DataFrame:
    """select_rf _summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        _description_
    y_columns : str
        _description_
    objective : str, optional
        _description_, by default "auto"

    Returns
    -------
    pd.DataFrame
        _description_
    """
    X = data.drop(columns=y_columns).copy()
    y = data[y_columns].copy()

    nans = pd.isna(X).sum().sum() + pd.isna(y).sum().sum()

    if type(objective) != str:
        raise TypeError(
            'objective must be a string: "auto", "regression" or "classification"'
        )
    if objective == "auto":
        objective = check_objective(y)
    model = get_classifier(objective=objective, y=y)
    response = {}

    model.fit(X, y)
    response['rf'] = {
        "support": dict(zip(model.feature_names_in_, model.feature_importances_ > np.quantile(model.feature_importances_, q=0.25))),
        "ranking": dict(zip(model.feature_names_in_, model.feature_importances_)),
    }
    return response


def get_classifier(y: pd.Series, objective: str = "classification"):
    assert objective in [
        "classification",
        "regression",
    ], 'Objective must be either "classsification" or "regression"'
    if objective == "regression":
        params = {
            "verbosity": 0,
            'learning_rate': 0.01,
            "gamma": 0.01,
            "max_depth": 5,
            "max_delta_step": 1,
            "subsample": 0.2,
            "colsample_bytree": 0.3,
            "lambda": 0.2,
            "alpha": 0.2,
            "max_leaves": 32,
        }
        model = XGBRegressor()
        model.set_params(**params)
        return model
    if objective == "classification":
        params = {
            "verbosity": 0,
            'learning_rate': 0.01,
            "gamma": 0.01,
            "max_depth": 5,
            "max_delta_step": 1,
            "subsample": 0.2,
            "colsample_bytree": 0.3,
            "lambda": 0.2,
            "alpha": 0.2,
            "max_leaves": 32,
            "scale_pos_weight": (y == 0).sum() / (y == 1).sum()
        }
        model = XGBClassifier()
        model.set_params(**params)
        return model


def get_boruta_classifier(objective: str = "classification"):
    assert objective in [
        "classification",
        "regression",
    ], 'Objective must be either "classsification" or "regression"'
    if objective == "regression":
        model = RandomForestRegressor(
            **models_settings['RandomForestRegressor'])
        return model
    elif objective == "classification":
        model = RandomForestClassifier(
            **models_settings['RandomForestClassifier'])
        return model


def add_cols(
    selection_results: Dict[str, Dict[str, bool]],
    selected_cols: List[str],
    serie_description: str,
) -> Dict[str, Dict[str, bool]]:

    for selected_col in selected_cols:
        if selected_col not in selection_results:
            selection_results[selected_col] = {}
        selection_results[selected_col][serie_description] = 1

    return selection_results


def color_one(val):
    if val == 1:
        return "background-color: gold"


def boruta_shap_select_cols(
    df: pd.DataFrame,
    y_columns: str,
    random_state: int,
    details: bool,
    objective: str = 'classification'
):

    df = df.copy()
    X = df.drop(columns=y_columns, inplace=False)
    y = df[y_columns]
    try:
        feature_selector = BorutaShap(
            model=get_classifier(objective=objective, y=y),
            importance_measure="Gini",
            classification=objective == 'classification',
        )
    except:
        raise AttributeError(
            'There are lines in BorutaShap code that prevent model from running. Go to BorutaShap.py and comment the lines 105-106. For deeper explanation go for Krystek')

    feature_selector.fit(
        X=X, y=y, n_trials=100, random_state=random_state, verbose=False
    )
    if details:
        feature_selector.plot(X_size=8, figsize=(12, 8), y_scale="log")
    order = (
        feature_selector.history_x.drop(
            columns=["Max_Shadow", "Min_Shadow",
                     "Mean_Shadow", "Median_Shadow"]
        ).mean()
        + abs(
            feature_selector.history_x.drop(
                columns=["Max_Shadow", "Min_Shadow",
                         "Mean_Shadow", "Median_Shadow"]
            ).min()
        )
    )
    order = order/sum(order)
    order = order.sort_values(ascending=False).to_dict()

    return list(feature_selector.Subset().columns), order, feature_selector.all_columns


def select_boruta_shap(
    data: pd.DataFrame,
    y_columns: str,
    n_folds: int = 5,
    random_state: int = 42,
    details: bool = False,
    objective: str = 'auto',
    splits: bool = False,
    score_threshold: int = 1
) -> List[str]:

    if type(objective) != str:
        raise TypeError(
            'objective must be a string: "auto", "regression" or "classification"'
        )

    if objective == "auto":
        objective = check_objective(data[y_columns])

    boruta_results = {}

    if splits:
        splits = split_train_test(data, n_folds=n_folds)
        # for split_number, split in enumerate(splits):

        #     boruta_results = add_cols(
        #         boruta_results,
        #         boruta_shap_select_cols(
        #             split.train_df,
        #             y_columns,
        #             random_state,
        #             details,
        #             objective=objective
        #         ),
        #         f"train_{split_number}",
        #     )
        #     boruta_df = pd.DataFrame.from_dict(
        #         boruta_results,
        #         orient="index",
        #         columns=[f"train_{split_number}" for split_number in range(
        #             0, len(splits))]
        #         + ["full_dataset"],
        #     )
    else:

        selected_cols, order, all_columns = boruta_shap_select_cols(
            data,
            y_columns,
            random_state,
            details,
            objective=objective
        )
    #     boruta_df = pd.DataFrame.from_dict(
    #         boruta_results,
    #         orient="index",
    #         columns=["full_dataset"],
    #     )

    # boruta_df.fillna(0, inplace=True)
    # for col in list(boruta_df.columns):
    #     boruta_df[col] = boruta_df[col].astype(int)
    # if details:
    #     display(boruta_df.style.applymap(color_one))

    columns = all_columns
    # score = boruta_df.sum(axis=1)

    response = {
        "boruta_shap": {
            "support": dict(zip(columns, np.isin(columns, selected_cols))),
            "ranking": order,
        }
    }
    return response


def rank_data(data: pd.DataFrame) -> Tuple[Any, Any]:
    """rank_data _summary_

    _extended_summary_

    Parameters
    ----------
    data : pd.DataFrame
        _description_
    Any : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    dm = skc.mkdm(
        data.values,
        [max] * len(data.columns),
        alternatives=data.index,
    )
    # inverter = invert_objectives.InvertMinimize()
    # dmt = inverter.transform(dm)
    wsm = simple.WeightedSumModel()
    # rank1 = wsm.evaluate(dmt)
    rank1 = wsm.evaluate(dm)

    mm = moora.RatioMOORA()
    # rank2 = mm.evaluate(dmt)
    rank2 = mm.evaluate(dm)
    return (rank1, rank2)


class feature_selection:
    """Klasa do wyboru istotnych zmiennych w zbiorze danych przed modelowaniem.
    Obsługuje problemy regresji, klasyfikacji binarnej, multiclass i multilabel classification.

    Attributes:
    ----------

        data: pd.DataFrame
            Zbiór danych wejściowych
        y_columns: List[str] = None
            Lista zmiennych zależnych. Jeżeli więcej niż 1, model będzie szukał istotnych zmiennych do klasyfikacji dla każdej zmiennej osobno.
        feature_support: Dict[Any] = None
            Do każdej zmiennej i każdej metody przypisany dict, wskazujący czy dana zmienna jest istotna w predykcji.
        feature_ranking: Dict[Any] = None
            Do każdej zmiennej i każdej metody przypisany dict, wskazujący pozycje zmiennej pod kątem istotności - gdzie 1 oznacza najbardziej istotną zmienną.
        filtered_data: pd.DataFrame = None
            Odfiltrowany zbiór danych (bez nieistotnych zmiennych)

    Methods:
    ----------
        recursively():
            opis
        boruta_shap():
            opis
        select_features(methods: List[str] = ["boruta_shap", "recursively"], how_many_positives: int = 1):
            opis

    """

    def __init__(self, data: DataSerieEDA = None):
        self.data_serie: DataSerieEDA = data
        self.data: pd.DataFrame = data.input_df[data.columns]
        self.imputed_data: pd.DataFrame = impute_data(
            data.input_df[data.columns])
        self.y_columns: List[str] = data.y_columns
        self.results: Dict[Any] = json.loads(
            str(dict.fromkeys(data.y_columns, {})).replace("'", '"')
        )
        self.feature_support: Dict[str, pd.DataFrame] = dict.fromkeys(
            data.y_columns, None
        )
        self.feature_ranking: Dict[str, pd.DataFrame] = dict.fromkeys(
            data.y_columns, None
        )
        self.summary_ranking: pd.DataFrame = None
        self.filtered_data: DataSerieEDA = None

    def recursively(self):
        for y in self.y_columns:
            recursive_response = select_recursively(
                self.imputed_data.drop(columns=list(
                    set(self.y_columns) - set([y]))).loc[pd.notna(self.data[y])], y
            )
            self.results[y].update(**recursive_response)
            print(f'Selected important features for variable {y}')

    def rf(self):
        for y in self.y_columns:
            recursive_response = select_rf(
                self.data.drop(columns=list(
                    set(self.y_columns) - set([y]))).loc[pd.notna(self.data[y])], y
            )
            self.results[y].update(**recursive_response)
            print(f'Selected important features for variable {y}')

    def boruta_shap(self):
        for y in self.y_columns:
            boruta_shap_response = select_boruta_shap(
                data=self.data.drop(columns=list(
                    set(self.y_columns) - set([y]))).loc[pd.notna(self.data[y])],
                y_columns=y
            )
            self.results[y].update(**boruta_shap_response)
            print(f'Selected important features for variable {y}')

    def select_features(
        self,
        methods: List[str] = ["recursively", "boruta_shap", "rf"],
        how_many_positives: int = 1,
    ):
        """select_features _summary_

        _extended_summary_

        Parameters
        ----------
        methods : List[str], optional
            _description_, by default ['recursively','boruta_shap']
        how_many_positives : int, optional
            _description_, by default 1
        """

        for method in methods:
            getattr(self, method)()
        for y, results in self.results.items():
            self.feature_support[y] = (
                pd.DataFrame.from_dict(
                    results).loc["support"].apply(pd.Series).T
            )
            self.feature_support[y]["sum"] = self.feature_support[y].sum(
                axis=1)
            self.feature_support[y]["confirmed"] = (
                self.feature_support[y]["sum"] >= how_many_positives
            )
            self.feature_ranking[y] = (
                pd.DataFrame.from_dict(
                    results).loc["ranking"].apply(pd.Series).T
            )
            # Tu strzel jakiegoś ifa, żeby brał tylko jak >1 wiersz
            try:
                wsm_rank, mm_rank = rank_data(self.feature_ranking[y])
                self.feature_ranking[y]["wsm_rank"] = wsm_rank.values
                self.feature_ranking[y]["mm_rank"] = mm_rank.values
            except:
                print(f'Unable to create ranking for {y}')
                self.feature_ranking[y]["wsm_rank"] = 0
                self.feature_ranking[y]["mm_rank"] = 0

        self.summary_ranking = pd.concat(self.feature_ranking.values(), axis=1)[
            ["wsm_rank", "mm_rank"]
        ]
        wsm_rank, mm_rank = rank_data(self.summary_ranking)
        self.summary_ranking["final_wsm_rank"] = wsm_rank.values
        self.summary_ranking["final_mm_rank"] = mm_rank.values
        self.summary_ranking = self.summary_ranking[[
            "final_wsm_rank", "final_mm_rank"]].rank(ascending=False)

    def filter_data(self, n_cols: int, method="mm"):
        """filter_data _summary_

        _extended_summary_

        Parameters
        ----------
        n_cols : int
            _description_
        method : str, optional
            _description_, by default "mm"
        """
        assert (
            self.feature_support is not None
        ), "Feature support and ranking is not calculated. Run `select_feature()` first!"
        self.filtered_data = DataSerieEDA(
            input_df=self.data.drop(
                columns=self.summary_ranking.loc[
                    (self.summary_ranking["final_" +
                     method + "_rank"] >= n_cols)
                ].index
            ),
            y_columns=self.y_columns,
            columns=self.data_serie.columns
        )

    def save_results(self):
        with open('feature_selection_results.pickle', 'wb') as handle:
            pickle.dump(vars(self), handle, protocol=pickle.HIGHEST_PROTOCOL)
