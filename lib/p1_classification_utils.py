
import numpy as np
import pandas as pd
import re
import prepare_data_series
from sklearn.metrics import (
    confusion_matrix,
    classification_report,
    accuracy_score,
    roc_auc_score,
    f1_score,
    matthews_corrcoef,
    precision_score, 
    recall_score,
    roc_curve
)
import seaborn as sns
import shap
import matplotlib.pyplot as plt
import lightgbm as lgb
from LGBM_metrics import auc_mu
from sklearn.metrics import roc_auc_score
from utils import check_objective
from argparse import ArgumentError
from sklearn.utils.class_weight import compute_sample_weight
from typing import List, Dict
from scipy.stats import ttest_ind, kstest
from scipy.stats import ks_2samp
import shutup
from itertools import compress

from typing import Dict
from dataclasses import dataclass
from read_data import save_dill
import datetime
from scipy.spatial import distance

import tensorflow as tf


shutup.please()

params = {
    "max_depth": 3,
    "num_leaves": 5,
    "n_estimators": 100,
    "learning_rate": 0.01,
    "class_weight": "balanced",
    "reg_alpha": 0.3,
    "reg_lambda": 0.3,
    "colsample_bytree": 0.1,
    "random_state": 1,
    "n_estimators": 100,
    "early_stopping_round": 15,
}

# apply threshold to positive probabilities to create labels
def to_labels(pos_probs, threshold):
	return (pos_probs >= threshold).astype('int')


def print_metrics(
        target_col: str,
        y_tests: np.array,
        y_predicted_test: np.array,
        predictions_test: np.array,
        y_trains: np.array,
        y_predicted_train: np.array,
        predictions_train: np.array,):
    objective = check_objective(
        pd.Series(y_trains)
    )
    if objective == "classification":
        print('-----------------------')
        print('TEST')
        print('Confusion Matrix')
        print(confusion_matrix(y_tests, y_predicted_test))
        print('Classification Report')
        print(classification_report(y_tests, y_predicted_test))
        print('Accuracy = ' + str(accuracy_score(y_tests, y_predicted_test)))
        print('F1 weighted = ' + str(f1_score(y_tests,
              y_predicted_test, average='weighted')))
        print('AUC = ' + str(auc_mu(pd.Series(y_tests).astype('category').cat.codes, predictions_test)))
        print('matthews_corrcoef = ' + str(matthews_corrcoef(y_tests, y_predicted_test)))

        print('-----------------------')
        print('TRAIN')
        print('Confusion Matrix')
        print(confusion_matrix(y_trains, y_predicted_train))
        print('Classification Report')
        print(classification_report(y_trains, y_predicted_train))
        print('Accuracy = ' + str(accuracy_score(y_trains, y_predicted_train)))
        print('F1 weighted = ' + str(f1_score(y_trains,
              y_predicted_train, average='weighted')))
        print('AUC = ' + str(auc_mu(pd.Series(y_trains).astype('category').cat.codes, predictions_train)))
        print('matthews_corrcoef = ' + str(matthews_corrcoef(y_trains, y_predicted_train)))
    else:
        raise NotImplementedError

def print_saved_metrics(
        results):
    objective = check_objective(
        pd.Series(results['y_train'])
    )
    if objective == "classification":
        print('-----------------------')
        print('TEST')
        print('Confusion Matrix')
        print(confusion_matrix(results['y_test'], results['y_predicted_test']))
        print('Classification Report')
        print(classification_report(results['y_test'], results['y_predicted_test']))
        print('Accuracy = ' + str(accuracy_score(results['y_test'], results['y_predicted_test'])))
        print('F1 weighted = ' + str(f1_score(results['y_test'],
              results['y_predicted_test'], average='weighted')))
        print('AUC = ' + str(roc_auc_score(results['y_test'], results['predictions_test'])))
        print('matthews_corrcoef = ' + str(matthews_corrcoef(results['y_test'], results['y_predicted_test'])))

        print('-----------------------')
        print('TRAIN')
        print('Confusion Matrix')
        print(confusion_matrix(results['y_train'], results['y_predicted_train']))
        print('Classification Report')
        print(classification_report(results['y_train'], results['y_predicted_train']))
        print('Accuracy = ' + str(accuracy_score(results['y_train'], results['y_predicted_train'])))
        print('F1 weighted = ' + str(f1_score(results['y_train'],
              results['y_predicted_train'], average='weighted')))
        print('AUC = ' + str(roc_auc_score(results['y_train'], results['predictions_train'])))
        print('matthews_corrcoef = ' + str(matthews_corrcoef(results['y_train'], results['y_predicted_train'])))
    else:
        raise NotImplementedError


def calculate_error_metrics(results, optimize_threshold=True):
    objective = check_objective(
        pd.Series(results['y_train'])
    )
    if (type(results['predictions_train'][0]) != float) & (type(results['predictions_train'][0])!=np.float32) &(type(results['predictions_train'][0])!=np.float64):
        results['predictions_train'] = results['predictions_train'][:, 1]
        results['predictions_test'] = results['predictions_test'][:, 1]
    if optimize_threshold:
        # keep probabilities for the positive outcome only
        # probs_train = results['predictions_train']
        # probs_test = results['predictions_test']
        # define thresholds
        # thresholds = np.arange(0, 1, 0.01)
        # # evaluate each threshold
        # scores = [matthews_corrcoef(results['y_test'], to_labels(probs_test, t)) for t in thresholds]
        # # get best threshold
        # ix = np.argmax(scores)

        # v2 - poprawki JWR
        fpr, tpr, thresholds = {}, {}, {}
        ix = {}
        for subset in ['test','train']:
            fpr[subset], tpr[subset], thresholds[subset] = roc_curve(results[f'y_{subset}'], results[f'predictions_{subset}'])
            distances = []
            for point in zip(fpr[subset], tpr[subset]):
                distances.append(distance.euclidean((0,1), point))
            ix[subset] = np.argmin(distances)

            results[f'y_predicted_{subset}'] = to_labels(results[f'predictions_{subset}'], thresholds[subset][ix[subset]]) 
    metrics={}
    if objective == "classification":
        for subset in ['test','train']:
            metrics[subset] = {}
            metrics[subset]['Precision'] = precision_score(results[f'y_{subset}'], results[f'y_predicted_{subset}'])
            metrics[subset]['Recall'] = recall_score(results[f'y_{subset}'], results[f'y_predicted_{subset}'])
            metrics[subset]['Accuracy'] = (accuracy_score(results[f'y_{subset}'], results[f'y_predicted_{subset}']))
            metrics[subset]['F1 weighted'] = f1_score(results[f'y_{subset}'],
                results[f'y_predicted_{subset}'], average='weighted')
            metrics[subset]['F1'] = f1_score(results[f'y_{subset}'],
                results[f'y_predicted_{subset}'], average=None)
            metrics[subset]['AUC'] =roc_auc_score(results[f'y_{subset}'], results[f'predictions_{subset}'], average = None)
            metrics[subset]['matthews_corrcoef'] = matthews_corrcoef(results[f'y_{subset}'], results[f'y_predicted_{subset}'])
            metrics[subset]['specificity'] = 1- fpr[subset][ix[subset]]
    else:
        raise NotImplementedError
    return metrics

def save_metrics(
    target_col: str,
    y_tests: np.array,
    y_predicted_test: np.array,
    predictions_test: np.array,
    y_trains: np.array,
    y_predicted_train: np.array,
    predictions_train: np.array,
    method: str,
    optimize_threshold = True,
    return_scores = False,
    verbose = 0
):
    objective = check_objective(
        pd.Series(y_trains)
    )
    if objective == "classification":
        if optimize_threshold:
            # keep probabilities for the positive outcome only
            probs_train = predictions_train[:, 1]
            probs_test = predictions_test[:, 1]
            # define thresholds
            thresholds = np.arange(0, 1, 0.01)
            # evaluate each threshold
            scores = [matthews_corrcoef(y_tests, to_labels(probs_test, t)) for t in thresholds]
            # get best threshold
            ix = np.argmax(scores)
            y_predicted_test = to_labels(probs_test, thresholds[ix])
            y_predicted_train = to_labels(probs_train, thresholds[ix])
            metrics = {}
            metrics[target_col] = {}
            metrics[target_col]["train_acc"] = accuracy_score(
                y_trains, y_predicted_train
            )
            metrics[target_col]["test_acc"] = accuracy_score(
                y_tests, y_predicted_test)
            metrics[target_col]["acc_overfit"] = metrics[target_col]["train_acc"] - \
                metrics[target_col]["test_acc"]
            metrics[target_col]["train_auc"] = auc_mu(
                pd.Series(y_trains).astype("category").cat.codes, predictions_train
            )
            metrics[target_col]["test_auc"] = auc_mu(
                pd.Series(y_tests).astype("category").cat.codes, predictions_test
            )
            metrics[target_col]["auc_overfit"] = metrics[target_col]["train_auc"] - \
                metrics[target_col]["test_auc"]
        else:
            metrics = {}
            metrics[target_col] = {}
            metrics[target_col]["train_acc"] = accuracy_score(
                y_trains, y_predicted_train
            )
            metrics[target_col]["test_acc"] = accuracy_score(
                y_tests, y_predicted_test)
            metrics[target_col]["acc_overfit"] = metrics[target_col]["train_acc"] - \
                metrics[target_col]["test_acc"]
            metrics[target_col]["train_auc"] = auc_mu(
                pd.Series(y_trains).astype("category").cat.codes, predictions_train
            )
            metrics[target_col]["test_auc"] = auc_mu(
                pd.Series(y_tests).astype("category").cat.codes, predictions_test
            )
            metrics[target_col]["auc_overfit"] = metrics[target_col]["train_auc"] - \
                metrics[target_col]["test_auc"]
    else:
        raise NotImplementedError
    if verbose>0:
        print_metrics(target_col, y_tests, y_predicted_test,
                predictions_test, y_trains, y_predicted_train, predictions_train)
    if return_scores:
        return metrics, dict(
            zip(
                ['y_tests', 'y_predicted_test', 'predictions_test', 'y_trains', 'y_predicted_train', 'predictions_train'],
                [y_tests, y_predicted_test, predictions_test, y_trains, y_predicted_train, predictions_train]
                ))
    return metrics


def get_lgbm_classifier(params: dict = params):
    model = lgb.LGBMClassifier(**params)
    return model

def calculate_predictions(model, X_test, X_train, y_test, y_train):
    predictions_test = model.predict_proba(X_test)[:,1]
    predictions_train = model.predict_proba(X_train)[:,1]

    # define thresholds
    thresholds = np.arange(0, 1, 0.01)
    # evaluate each threshold
    scores = [matthews_corrcoef(y_train, to_labels(predictions_train, t)) for t in thresholds]
    # get best threshold
    ix = np.argmax(scores)
    y_predicted_test = to_labels(predictions_test, thresholds[ix])
    y_predicted_train = to_labels(predictions_train, thresholds[ix])
    results = dict(
            zip(
                ['y_test', 'y_predicted_test', 'predictions_test', 'y_train', 'y_predicted_train', 'predictions_train'],
                [y_test, y_predicted_test, predictions_test, y_train, y_predicted_train, predictions_train]
                ))
    return results


def train_folds(target_col, train_cols, data, verbose=1, calculate_shap = False, **kwargs):
    shap_values = []
    folds = prepare_data_series.split_fold_train_test(data, target_col, 5)
    predictions_test = np.empty((0, 2))
    y_predicted_test = np.empty((0, 1)).reshape(0)
    predictions_train = np.empty((0, 2))
    y_predicted_train = np.empty((0, 1)).reshape(0)
    y_trains = np.empty((0, 1)).reshape(0)
    y_tests = np.empty((0, 1)).reshape(0)
    models_folds = []
    for fold in folds:
        model = get_lgbm_classifier(**kwargs)
        X_train = fold.train_df[train_cols].copy()
        X_train.rename(
            columns=dict(
                zip(
                    X_train.columns,
                    X_train.columns.str.normalize("NFKD")
                    .str.encode("ascii", errors="ignore")
                    .str.decode("utf-8")
                    .tolist(),
                )
            ),
            inplace=True
        )
        X_train.rename(columns=lambda x: re.sub('[^A-Za-z0-9_+-]+', '', x),
                       inplace=True)
        X_test = fold.test_df[train_cols].copy()
        X_test.rename(
            columns=dict(
                zip(
                    X_test.columns,
                    X_test.columns.str.normalize("NFKD")
                    .str.encode("ascii", errors="ignore")
                    .str.decode("utf-8")
                    .tolist(),
                )
            ),
            inplace=True
        )
        X_test.rename(columns=lambda x: re.sub('[^A-Za-z0-9_+-]+', '', x),
                      inplace=True)
        # .rename(lambda x:re.sub('[^A-Za-z0-9_+-]+', '', x))
        y_train = fold.train_df[target_col]
        # .rename(lambda x:re.sub('[^A-Za-z0-9_+-]+', '', x))
        y_test = fold.test_df[target_col]
        model.fit(X=X_train, y=y_train, eval_set=[
                  (X_test, y_test)], eval_metric='logloss', callbacks = [lgb.log_evaluation(0)])
        predictions_test = np.r_[predictions_test, model.predict_proba(X_test)]
        y_predicted_test = np.r_[y_predicted_test, model.predict(X_test)]
        predictions_train = np.r_[
            predictions_train, model.predict_proba(X_train)]
        y_predicted_train = np.r_[y_predicted_train, model.predict(X_train)]
        y_trains = np.r_[y_trains, y_train]
        y_tests = np.r_[y_tests, y_test]
        models_folds.append(model)

    i = 0
    if calculate_shap:
        test_dfs = []
        for fold in folds:
            test_df = fold.test_df[train_cols].copy()
            test_df.rename(
                columns=dict(
                    zip(
                        test_df.columns,
                        test_df.columns.str.normalize("NFKD")
                        .str.encode("ascii", errors="ignore")
                        .str.decode("utf-8")
                        .tolist(),
                    )
                ),
                inplace=True
            )
            test_df.rename(columns=lambda x: re.sub('[^A-Za-z0-9_+-]+', '', x),
                        inplace=True)
            explainer = shap.TreeExplainer(models_folds[i], data = test_df,feature_perturbation="interventional", model_output='probability') 
            # shap.TreeExplainer(models_folds[i])
            shap_values.append(explainer.shap_values(test_df))
            test_dfs.append(test_df)

            i = +1
        shap_values = np.r_[tuple(shap_values)]
        test_dfs = pd.concat(test_dfs)
        if verbose > 0:
            # shap.summary_plot(shap_values, test_dfs,plot_type="bar")
            # plt.show()
            shap.summary_plot(shap_values, test_dfs)
            plt.show()
        return save_metrics(target_col, y_tests, y_predicted_test, predictions_test, y_trains, y_predicted_train, predictions_train, 'lgbm', verbose=verbose), shap_values, test_dfs
    return save_metrics(target_col, y_tests, y_predicted_test, predictions_test, y_trains, y_predicted_train, predictions_train, 'lgbm', verbose=verbose)

from sklearn.neighbors import KNeighborsClassifier

def train_folds_imputation(target_col, train_cols, data, verbose=1, calculate_shap = False,model_type='lgb', random_state:int = 42 ,  **kwargs):
    shap_values = []
    folds = prepare_data_series.split_fold_train_test(data, target_col, 5, random_state=random_state)
    predictions_test = np.empty((0, 2))
    y_predicted_test = np.empty((0, 1)).reshape(0)
    predictions_train = np.empty((0, 2))
    y_predicted_train = np.empty((0, 1)).reshape(0)
    y_trains = np.empty((0, 1)).reshape(0)
    y_tests = np.empty((0, 1)).reshape(0)
    models_folds = []
    for fold in folds:
        if model_type=='lgb':
            model = get_lgbm_classifier(**kwargs)
        elif model_type=='knn':
            model = KNeighborsClassifier(30)
        X_train = fold.train_df[train_cols].copy()
        X_train.rename(
            columns=dict(
                zip(
                    X_train.columns,
                    X_train.columns.str.normalize("NFKD")
                    .str.encode("ascii", errors="ignore")
                    .str.decode("utf-8")
                    .str.capitalize()
                    .tolist(),
                )
            ),
            inplace=True
        )
        X_train.rename(columns=lambda x: re.sub('[^A-Za-z0-9 _+-]+', '', x),
                       inplace=True)
        X_test = fold.test_df[train_cols].copy()
        X_test.rename(
            columns=dict(
                zip(
                    X_test.columns,
                    X_test.columns.str.normalize("NFKD")
                    .str.encode("ascii", errors="ignore")
                    .str.decode("utf-8")
                    .str.capitalize()
                    .tolist(),
                )
            ),
            inplace=True
        )
        X_test.rename(columns=lambda x: re.sub('[^A-Za-z0-9 _+-]+', '', x),
                      inplace=True)
        # .rename(lambda x:re.sub('[^A-Za-z0-9_+-]+', '', x))
        y_train = fold.train_df[target_col]
        # .rename(lambda x:re.sub('[^A-Za-z0-9_+-]+', '', x))
        y_test = fold.test_df[target_col]
        if model_type=='lgb':
            model.fit(X=X_train, y=y_train, eval_set=[
                  (X_test, y_test)], eval_metric='logloss', callbacks = [lgb.log_evaluation(0)])
        elif model_type=='knn':
            model.fit(X=X_train, y=y_train)
        
        predictions_test = np.r_[predictions_test, model.predict_proba(X_test)]
        y_predicted_test = np.r_[y_predicted_test, model.predict(X_test)]
        predictions_train = np.r_[
            predictions_train, model.predict_proba(X_train)]
        y_predicted_train = np.r_[y_predicted_train, model.predict(X_train)]
        y_trains = np.r_[y_trains, y_train]
        y_tests = np.r_[y_tests, y_test]
        models_folds.append(model)

    i = 0
    if calculate_shap:
        test_dfs = []
        for fold in folds:
            test_df = fold.test_df[train_cols].copy()
            test_df.rename(
                columns=dict(
                    zip(
                        test_df.columns,
                        test_df.columns.str.normalize("NFKD")
                        .str.encode("ascii", errors="ignore")
                        .str.decode("utf-8")
                        .str.capitalize()
                        .tolist(),
                    )
                ),
                inplace=True
            )
            test_df.rename(columns=lambda x: re.sub('[^A-Za-z0-9 _+-]+', '', x),
                        inplace=True)
            explainer = shap.TreeExplainer(models_folds[i], data = test_df,feature_perturbation="interventional", model_output='probability') 
            # shap.TreeExplainer(models_folds[i])
            shap_values.append(explainer.shap_values(test_df))
            test_dfs.append(test_df)

            i = +1
        shap_values = np.r_[tuple(shap_values)]
        test_dfs = pd.concat(test_dfs)
        if verbose > 0:
            # shap.summary_plot(shap_values, test_dfs,plot_type="bar")
            # plt.show()
            shap.summary_plot(shap_values, test_dfs)
            plt.show()
    results = dict(
        zip(
            ['y_test', 'y_predicted_test', 'predictions_test', 'y_train', 'y_predicted_train', 'predictions_train'],
            [y_tests, y_predicted_test, predictions_test, y_trains, y_predicted_train, predictions_train]
            ))
    return calculate_error_metrics(results), results

def get_DNN_model(params, input_dim):
    model = tf.keras.models.Sequential()
    model.add(tf.keras.layers.Dense(params['HP_NUM_UNITS'],
            input_dim=input_dim,
            activation=tf.keras.activations.elu))
    for i in range(params['HP_NUM_LAYERS']):
        model.add(tf.keras.layers.Dense(params['HP_NUM_UNITS'],
                        activation=params['HP_ACTIVATION_FUNCTION']))

    model.add(tf.keras.layers.Dense(1, activation='sigmoid',
                    kernel_initializer='normal'))


    # define avaluation and optimization criteria
    model.compile(loss='binary_crossentropy',
                optimizer=tf.keras.optimizers.Adam(learning_rate = tf.keras.optimizers.schedules.ExponentialDecay(
                            initial_learning_rate=params['HP_LR'],
                            decay_steps=1,
                            decay_rate=0.99)),  
                metrics=['AUC'])
    return model

def fit_DNN_model(
    model,
    train_X,
    train_y,
    test_X,
    test_y,
    verbose=0
    ):
    early_stop = tf.keras.callbacks.EarlyStopping(
        monitor="val_auc",
        min_delta=0.001,
        patience=30,
        verbose=0,
        mode="max",
        restore_best_weights=True,
    )
    model.fit(train_X,
            train_y,             
            batch_size=128,
            validation_data=(test_X, test_y),
            epochs=100,
            #class_weight = class_weights,
            callbacks=[early_stop],
            verbose=verbose) 
    return model

def train_folds_imputation_DNN(target_col, train_cols, data, params, verbose=0,**kwargs):
    folds = prepare_data_series.split_fold_train_test(data, target_col, 5)
    predictions_test = np.empty((0, ))
    predictions_train = np.empty((0, ))
    y_trains = np.empty((0, 1)).reshape(0)
    y_tests = np.empty((0, 1)).reshape(0)
    for fold in folds:
        X_train = fold.train_df[train_cols].copy()
        X_test = fold.test_df[train_cols].copy()
        y_train = fold.train_df[target_col]
        y_test = fold.test_df[target_col]
        model = get_DNN_model(params,input_dim=X_train.shape[1])
        model = fit_DNN_model(model=model, train_X=X_train, train_y=y_train, test_X=X_test, test_y=y_test, verbose=verbose)
        
        predictions_test = np.r_[predictions_test, model.predict(X_test).reshape(X_test.shape[0])]
        predictions_train = np.r_[
            predictions_train, model.predict(X_train).reshape(X_train.shape[0])]
        y_trains = np.r_[y_trains, y_train]
        y_tests = np.r_[y_tests, y_test]

    results = dict(
        zip(
            ['y_test', 'y_predicted_test', 'predictions_test', 'y_train', 'y_predicted_train', 'predictions_train'],
            [y_tests, None, predictions_test, y_trains, None, predictions_train]
            ))
    return calculate_error_metrics(results)




def calculate_metrics(feature_selection_results, rank_type: str, rank_int: int):
    if rank_type == 'rf':
        metrics = []
        for y_col in feature_selection_results['y_columns']:
            data = feature_selection_results['data']
            target_col = y_col
            train_cols = feature_selection_results['feature_ranking'][target_col].sort_values(
                'rf', ascending=False).head(rank_int).index
            print(target_col)
            metrics.append(train_folds(target_col, train_cols,
                           data))
    elif rank_type == 'wsm':
        metrics = []
        for y_col in feature_selection_results['y_columns']:
            data = feature_selection_results['data']
            target_col = y_col
            train_cols = feature_selection_results['feature_ranking'][target_col].query(
                f'wsm_rank < {rank_int}').index
            print(target_col)
            metrics.append(train_folds(target_col, train_cols,
                           data))
    else:
        raise ArgumentError(message='rank_type must be rf or wsm')
    return metrics


def check_random_impact(target_col, train_cols, data, params, verbose=0):
    metric_list = {}
    for i in range(25):
        params['random_state'] = i
        metric_list[i] = train_folds(
            target_col=target_col, train_cols=train_cols, data=data, verbose=0, params=params)[target_col]

    return pd.DataFrame.from_dict(metric_list).T


def find_optimal_subset(feature_selection_results: Dict, target_col: str, feature_importance_source=None, run_random_impact_measurement: bool = False, verbose: int = 0, drop_every_column: bool = True, **kwargs) -> Dict:
    """find_optimal_subset _summary_

    _extended_summary_

    Parameters
    ----------
    target_col : str
        _description_
    train_cols : List[str]
        _description_
    data : pd.DataFrame
        _description_
    verbose : int, optional
        _description_, by default 0

    Returns
    -------
    Dict
        _description_
    """
    data = feature_selection_results['data']
    data = data.loc[data[target_col].notna()]
    columns_feature_selection = feature_selection_results['feature_ranking'][target_col].index.tolist(
    )
    columns_to_be_checked = []
    response = {}
    improved_training_response = {}
    if feature_importance_source is not None:
        feature_importance_source.columns = ['features', 'rf', 'boruta_shap', 'wsm_rank', 'mm_rank',
                                             'Do dalszego modelowania']
        feature_importance_source = feature_importance_source.sort_values(
            by='wsm_rank', ascending=False)
        missing_cols = sorted(set(columns_feature_selection)-set(
            feature_importance_source['features'].tolist()), key=columns_feature_selection.index)
        surplus_cols = sorted(set(feature_importance_source['features'].tolist())-set(
            columns_feature_selection), key=feature_importance_source['features'].tolist().index)
        initial_important_cols = sorted(set(feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] > -1][
            "features"
        ].tolist())-set(
            surplus_cols), key=feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] > -1][
            "features"
        ].tolist().index)
        train_cols_full = (
            initial_important_cols
            + missing_cols
        )
        columns_to_be_checked = sorted(set(feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] == 0][
            "features"
        ].tolist())-set(
            surplus_cols), key=feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] == 0][
            "features"
        ].tolist().index)

        columns_to_be_used = sorted(set(feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] == 1][
            "features"
        ].tolist())-set(surplus_cols), key=feature_importance_source.loc[feature_importance_source["Do dalszego modelowania"] == 1][
            "features"
        ].tolist().index) + missing_cols
    if run_random_impact_measurement:
        random_impact_initial = check_random_impact(target_col, train_cols_full,
                                                    data, params=params)
        response['initial_model_metrics'] = random_impact_initial
        best_auc = random_impact_initial['test_auc']
        if verbose > 0:
            print(
                f'Initial model: test auc = {np.median(random_impact_initial["test_auc"])}, auc_overfit = {np.median(random_impact_initial["auc_overfit"])}')
        print('Checking the impact of columns with importance 0')
        train_cols = train_cols_full
        for column in columns_to_be_checked:
            train_cols_subset = list(set(train_cols) - set([column]))
            train_metrics = check_random_impact(target_col, train_cols_subset,
                                                data, params=params)
            improved_training = ks_2samp(
                train_metrics['test_auc'], best_auc, alternative='greater')
            remove_column = (improved_training[1] > 0.01) | (
                (np.median(best_auc) - np.median(train_metrics["test_auc"])) < 0.01)
            if verbose > 0:
                print(
                    f'{column}: test auc = {np.median(train_metrics["test_auc"])}, auc_overfit = {np.median(train_metrics["auc_overfit"])}, p_value = {improved_training[1]}, removed = {remove_column}')

            if drop_every_column:
                best_auc = train_metrics['test_auc']
                train_cols = train_cols_subset
            response[column] = train_metrics
            improved_training_response[column] = [
                improved_training, remove_column]

        print('Checking the impact of columns with importance 1')
        for column in columns_to_be_used:
            train_cols_subset = list(set(train_cols) - set([column]))
            train_metrics = check_random_impact(target_col, train_cols_subset,
                                                data, params=params)
            improved_training = ks_2samp(
                train_metrics['test_auc'], best_auc, alternative='greater')
            remove_column = (improved_training[1] > 0.01) | (
                (np.median(best_auc) - np.median(train_metrics["test_auc"])) < 0.01)
            if verbose > 0:
                print(
                    f'{column}: test auc = {np.median(train_metrics["test_auc"])}, auc_overfit = {np.median(train_metrics["auc_overfit"])}, p_value = {improved_training[1]}, removed = {remove_column}')
            if drop_every_column:
                best_auc = train_metrics['test_auc']
                train_cols = train_cols_subset
            response[column] = train_metrics
            improved_training_response[column] = [
                improved_training, remove_column]
        return response, train_cols_full, train_cols, improved_training_response
    else:
        random_impact_initial = check_random_impact(target_col, train_cols_full,
                                                    data, params=params)
        stdev = 3*random_impact_initial['test_auc'].std()
        initial_model_metrics = random_impact_initial.apply(
            np.median).to_dict()
        response['initial_model_metrics'] = initial_model_metrics
        response['initial_model_metrics']['drop'] = True
        best_auc = np.median(random_impact_initial["test_auc"])
        if verbose > 0:
            print(
                f'initial_model: test auc = {initial_model_metrics["test_auc"]}, auc_overfit = {initial_model_metrics["auc_overfit"]}, stdev = {stdev}')
        print('Checking the impact of columns with importance 0')
        train_cols = train_cols_full
        for column in columns_to_be_checked:
            train_cols_subset = list(set(train_cols) - set([column]))
            train_metrics = train_folds(target_col, train_cols_subset,
                                        data, params=params, verbose=0)[target_col]
            improved_training = (best_auc - train_metrics["test_auc"]) < 0.015
            if verbose > 0:
                print(
                    f'{column}: test auc = {train_metrics["test_auc"]}, auc_overfit = {train_metrics["auc_overfit"]}, drop = {improved_training}')
            if improved_training:
                best_auc = max(train_metrics['test_auc'], best_auc)
                train_cols = train_cols_subset
            response[column] = train_metrics
            response[column]['drop'] = improved_training

        print('Checking the impact of columns with importance 1')
        for column in columns_to_be_used:
            train_cols_subset = list(set(train_cols) - set([column]))
            train_metrics = train_folds(target_col, train_cols_subset,
                                        data, params=params, verbose=0)[target_col]
            improved_training = (best_auc - train_metrics["test_auc"]) < 0.015
            if verbose > 0:
                print(
                    f'{column}: test auc = {train_metrics["test_auc"]}, auc_overfit = {train_metrics["auc_overfit"]}, drop = {improved_training}')
            if improved_training:
                best_auc = max(train_metrics['test_auc'], best_auc)
                train_cols = train_cols_subset
            response[column] = train_metrics
            response[column]['drop'] = improved_training
    return response, train_cols_full, train_cols

def find_optimal_subset_paper(
        data: pd.DataFrame, 
        target_col: str, 
        columns_feature_selection: pd.DataFrame,
        verbose: int = 0,
        **kwargs) -> Dict:
    """find_optimal_subset _summary_

    _extended_summary_

    Parameters
    ----------
    target_col : str
        _description_
    train_cols : List[str]
        _description_
    data : pd.DataFrame
        _description_
    verbose : int, optional
        _description_, by default 0

    Returns
    -------
    Dict
        _description_
    """
    data = data.loc[data[target_col].notna()]
    columns_to_be_checked = []
    response = {}
    improved_training_response = {}

    train_cols_full = columns_feature_selection.feature.loc[
            (columns_feature_selection.selected_for_modelling > -1)].to_list()
    columns_to_be_checked = columns_feature_selection.feature.loc[
        (columns_feature_selection.selected_for_modelling == 0)].to_list()
        


    random_impact_initial = check_random_impact(target_col, train_cols_full,
                                                data, params=params)
    response['initial_model_metrics'] = random_impact_initial
    best_auc = random_impact_initial["test_auc"]
    q75, q25 = np.percentile(best_auc, [75, 25])
    iqr = np.subtract(q75, q25)
    best_auc = list(compress(best_auc,best_auc<(q75+1.5*iqr)))

    if verbose > 0:
        print(
            f'Initial model: test auc = {np.median(best_auc)}, auc_overfit = {np.median(random_impact_initial["auc_overfit"])}')
    print('Checking the impact of columns with importance 0')
    train_cols = train_cols_full
    for column in columns_to_be_checked:
        train_cols_subset = list(set(train_cols) - set([column]))
        train_metrics = check_random_impact(target_col, train_cols_subset,
                                            data, params=params)
        current_auc = train_metrics['test_auc']
        q75, q25 = np.percentile(current_auc, [75, 25])
        iqr = np.subtract(q75, q25)
        current_auc = list(compress(current_auc,current_auc<(q75+1.5*iqr)))
        improved_training = ks_2samp(
            current_auc, best_auc, alternative='greater')
        remove_column = (improved_training[1] > 0.01)# | ((np.median(best_auc) - np.median(train_metrics["test_auc"])) < 0.01)
        if verbose > 0:
            print(
                f'{column}: test auc = {np.median(current_auc)}, auc_overfit = {np.median(train_metrics["auc_overfit"])}, p_value = {improved_training[1]}, removed = {remove_column}')

        if remove_column:
            best_auc = current_auc
            train_cols = train_cols_subset
        response[column] = train_metrics
        improved_training_response[column] = [
            improved_training, remove_column]
    return response, train_cols_full, train_cols

def recursive_feature_selection(
        data: pd.DataFrame, 
        target_col: str, 
        columns_feature_selection: pd.Series,
        verbose: int = 0,
        **kwargs) -> Dict:
    """find_optimal_subset _summary_

    _extended_summary_

    Parameters
    ----------
    target_col : str
        _description_
    train_cols : List[str]
        _description_
    data : pd.DataFrame
        _description_
    verbose : int, optional
        _description_, by default 0

    Returns
    -------
    Dict
        _description_
    """
    data = data.loc[data[target_col].notna()]
    columns_to_be_checked = []
    response = {}
    improved_training_response = {}

    columns_feature_selection.sort_values(ascending=True, inplace = True)

    train_cols_full = columns_feature_selection.index.tolist()
    columns_to_be_checked = columns_feature_selection.index.tolist()
        


    random_impact_initial = check_random_impact(target_col, train_cols_full,
                                                data, params=params)
    response['initial_model_metrics'] = random_impact_initial
    best_auc = random_impact_initial["test_auc"]
    q75, q25 = np.percentile(best_auc, [75, 25])
    iqr = np.subtract(q75, q25)
    best_auc = list(compress(best_auc,best_auc<(q75+1.5*iqr)))
    if verbose > 0:
        print(
            f'Initial model: test auc = {np.median(random_impact_initial["test_auc"])}, auc_overfit = {np.median(random_impact_initial["auc_overfit"])}')
    train_cols = train_cols_full
    for column in columns_to_be_checked:
        train_cols_subset = list(set(train_cols) - set([column]))
        train_metrics = check_random_impact(target_col, train_cols_subset,
                                            data, params=params)
        current_auc = train_metrics['test_auc']
        q75, q25 = np.percentile(current_auc, [75, 25])
        iqr = np.subtract(q75, q25)
        current_auc = list(compress(current_auc,current_auc<(q75+1.5*iqr)))
        improved_training = ks_2samp(
            current_auc, best_auc, alternative='greater')
        remove_column = (improved_training[1] > 0.01)# | ((np.median(best_auc) - np.median(train_metrics["test_auc"])) < 0.01)
        if verbose > 0:
            print(
                f'{column}: test auc = {np.median(current_auc)}, auc_overfit = {np.median(train_metrics["auc_overfit"])}, p_value = {improved_training[1]}, removed = {remove_column}')

        if remove_column:
            best_auc = current_auc
            train_cols = train_cols_subset
        response[column] = train_metrics
        improved_training_response[column] = [
            improved_training, remove_column]
    return response, train_cols_full, train_cols


search_types = ["z_usuwaniem", "bez_usuwania"]
subset_types = ["optimal", "minimal", "maximal"]


@dataclass
class subsets:
    diagnoza: str
    search_type: str
    subset_type: str

    def __str__(self):
        return f"{self.diagnoza}_{self.search_type}_{self.subset_type}"

    def __gain__(self):
        return f"{self.diagnoza}_{self.search_type}"



class diagnozy_subsets:
    def __init__(
        self,
        diagnozy=None,
        feature_selection_results_frequency_level_05: Dict = None,
        wyniki_z_usuwaniem: Dict = None,
        wyniki_bez_usuwania: Dict = None,
        search_types: List = search_types,
        subset_types: List = subset_types,
        params: Dict = params,
    ):
        self.diagnozy = diagnozy
        self.data = feature_selection_results_frequency_level_05["data"]
        self.wyniki_z_usuwaniem = wyniki_z_usuwaniem
        self.wyniki_bez_usuwania = wyniki_bez_usuwania
        self.search_types = search_types
        self.subset_types = subset_types
        self.params = params
        self.gains = {}
        self.selected_columns = {}
        self.random_impact = {}
        self.metrics = {}
        self.shap_values = {}
        self.test_df = {}
        self.importances = {}
        self.cutoffs = dict(zip(subset_types, [0.5, 0.75, 0.25]))

    def calculate_gains(self, diagnoza, search_type="z_usuwaniem"):
        subset = subsets(diagnoza, search_type, "")
        if search_type == "z_usuwaniem":
            ## Wyniki z usuwaniem
            self.gains[subset.__gain__()] = {}
            for key, value in self.wyniki_z_usuwaniem[diagnoza]["response"].items():
                self.gains[subset.__gain__()][key] = value.test_auc.median()

            self.gains[subset.__gain__()] = pd.Series(
                self.gains[subset.__gain__()]
            ).shift(1) - pd.Series(self.gains[subset.__gain__()])
        elif search_type == "bez_usuwania":
            ## Wyniki bez usuwania
            self.gains[subset.__gain__()] = {}
            for key, value in self.wyniki_bez_usuwania[diagnoza]["response"].items():
                self.gains[subset.__gain__()][key] = value.test_auc.median()
            self.gains[subset.__gain__()] = (
                -pd.Series(self.gains[subset.__gain__()]).drop("initial_model_metrics")
                + pd.Series(self.gains[subset.__gain__()])
                .drop("initial_model_metrics")
                .median()
            )

    def calculate_subsets(
        self,
        diagnoza,
        search_type,
        subset_type,
    ):
        subset = subsets(diagnoza=diagnoza, search_type=search_type, subset_type=subset_type)
        self.selected_columns[subset.__str__()] = None
        self.random_impact[subset.__str__()] = None
        self.metrics[subset.__str__()] = None
        self.shap_values[subset.__str__()] = None
        self.test_df[subset.__str__()] = None
        self.importances[subset.__str__()] = None

        cutoff = (
            self.gains[subset.__gain__()]
            .loc[self.gains[subset.__gain__()] > 0]
            .quantile(self.cutoffs[subset_type])
        )
        self.selected_columns[subset.__str__()] = (
            self.gains[subset.__gain__()]
            .loc[self.gains[subset.__gain__()] > cutoff]
            .index.tolist()
        )
        self.selected_columns[subset.__str__()].__len__()
        self.random_impact[subset.__str__()] = check_random_impact(
            target_col=diagnoza,
            train_cols=self.selected_columns[subset.__str__()],
            data=self.data.loc[self.data[diagnoza].notna()],
            verbose=0,
            params=params,
        )
        max_rs = self.random_impact[subset.__str__()].loc[
            self.random_impact[subset.__str__()].test_auc
            == self.random_impact[subset.__str__()].test_auc.max()
        ]
        self.params["random_state"] = max_rs.index.values[0]
        self.random_impact[subset.__str__()].loc[
            self.random_impact[subset.__str__()].test_auc
            == self.random_impact[subset.__str__()].test_auc.max()
        ]
        (
            self.metrics[subset.__str__()],
            self.shap_values[subset.__str__()],
            self.test_df[subset.__str__()]
        ) = train_folds(
            target_col=diagnoza,
            train_cols=self.selected_columns[subset.__str__()],
            data=self.data.loc[self.data[diagnoza].notna()],
            verbose=0,
            params=params,
            calculate_shap=True,
        )
        self.metrics[subset.__str__()] = self.metrics[subset.__str__()][diagnoza]
        self.importances[subset.__str__()] = pd.Series(
            abs(self.shap_values[subset.__str__()]).mean(axis=0),
            index=self.selected_columns[subset.__str__()],
        )

        print(
            f"{subset.__str__()}: auc = {self.random_impact[subset.__str__()].test_auc.median()}, overfit = {self.random_impact[subset.__str__()].auc_overfit.median()}, n_cols = {self.selected_columns[subset.__str__()].__len__()}"
        )

    def run(self):
        for diagnoza in self.diagnozy:
            for search_type in self.search_types:
                self.calculate_gains(diagnoza, search_type)
                for subset_type in self.subset_types:
                    self.calculate_subsets(
                        diagnoza=diagnoza,
                        search_type=search_type,
                        subset_type=subset_type,
                    )
        save_dill(self,f'diagnozy_subsets_{datetime.datetime.now().date()}','data_cbr')


from prepare_data_series import split_train_test

def train_prod(target_col, train_cols, data, **kwargs):

    X_train, X_test,y_train ,y_test = split_train_test(
        input_df = data[train_cols], 
        target_col = target_col,
        test_size=0.2)
    model = get_lgbm_classifier(**kwargs)
    model.fit(X=X_train, y=y_train, eval_set=[
                (X_test, y_test)], eval_metric='logloss', callbacks = [lgb.log_evaluation(0)])
    results = calculate_predictions(
        model, X_test, X_train, y_test, y_train
    )
    print_saved_metrics(results)
    return model, results 