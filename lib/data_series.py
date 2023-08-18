from typing import Dict, Tuple, List
from dataclasses import dataclass
import pandas as pd

from utils import TrainTestSplit, split_train_test


@dataclass
class DataSerie:
    input_df: pd.DataFrame
    target_col: str
    splits: List[TrainTestSplit]
    columns: List[str]
