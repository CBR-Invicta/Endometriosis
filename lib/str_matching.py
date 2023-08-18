# Copyright (c) 2018 luozhouyang
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# ==============================================================================
# We observe that substitution errors (1.65%) are more frequent
# than omission (0.8%) or insertion (0.67%) errors. Comparing
# fast with slow typists, we can see that there is a particularly
# large difference in substitution error rate, with large variation
# among slow typists (d = 1.57), while trained and untrained
# typists are similar in errors. We hypothesise that substitution
# errors are common when participants are less consistent in
# typing and have a poor mental representation of the fingers’
# position. This result stands in contrast to prior studies in which
# insertion errors were commonly found to be the most frequent
# error type [12, 16, 33].
# 
# https://userinterfaces.aalto.fi/136Mkeystrokes/resources/chi-18-analysis.pdf

from tokenize import String
from xmlrpc.client import Boolean

from numpy import deprecate
from sklearn.utils import deprecated
from strsimpy.damerau import Damerau
from strsimpy.jaro_winkler import JaroWinkler
from abydos.phonetic import Haase
from typing import Tuple
import pandas as pd
import os

def ensemble_sim(
    lex_sim: float,
    phon_sim: float,
    WEIGHT_LEX_H: float = 0.4,
    WEIGHT_PHO_H: float = 0.6,
    WEIGHT_LEX_L: float = 0.65,
    WEIGHT_PHO_L: float = 0.35,
    PHO_THRESHOLD: float = 0.75,
) -> float:
    """Ważone połącznie metod string matchingu leksykalnego z fonetycznym,
        jeśli podobieństwo fonetyczne jest wyższe niż dany PHO_THRESHOLD,
        waga podobieństwa fonetycznego jest dominująca.

    Args:
        lex_sim (float): Wartość podobieństwa z metody leksykalnej - przedział (0, 1).
        phon_sim (float): Wartość podobieństwa z metody fonetycznej - przedział (0, 1).
        WEIGHT_LEX_H (float, optional): Waga podobieństwa leksykalnego z dominującym fonetycznym. Defaults to 0.4.
        WEIGHT_PHO_H (float, optional): Waga podobieństwa fonetycznego z dominującym fonetycznym. Defaults to 0.6.
        WEIGHT_LEX_L (float, optional): Waga podobieństwa leksykalnego z dominującym leksykalnym. Defaults to 0.65.
        WEIGHT_PHO_L (float, optional): Waga podobieństwa fonetycznego z dominującym leksykalnym. Defaults to 0.35.
        PHO_THRESHOLD (float, optional): Próg podobieństwa fonetycznego. Defaults to 0.75.

    Returns:
        float: Wynikowa wartość ważonego podobieństwa - przedział (0, 1).
    """

    if phon_sim >= PHO_THRESHOLD:
        return lex_sim * WEIGHT_LEX_H + phon_sim * WEIGHT_PHO_H
    else:
        return lex_sim * WEIGHT_LEX_L + phon_sim * WEIGHT_PHO_L

def match_words(word_base: str, word_matched: str, result_threshold: float = 0.75) -> Tuple[bool, float]:
    """Matchowanie wyrazów z progowaniem - Ważony Levenshtein-Damerau, Haase Phonetik(Lev-Dam),
        Jaro-Winkler dla skrótow - wyrazów o długości krótszej niż ABBREVIATION_THRESHOLD wyrazu bazowego.

    Args:
        word_base (str): Bazowy wyraz.
        word_matched (str): Wyraz porównywany z bazowym.
        result_threshold (float, optional): Próg dla pozytywnego dopasowania wyrazów. Defaults to 0.75.

    Returns:
        Tuple[bool, float]: Przekroczenie progu dopasowania, wartość podobieństwa wyrazów.
    """

    ABBREVIATION_THRESHOLD = 0.5

    phonetic_haase = Haase()
    damerau_matching = Damerau()
    wei_mod_matching = WeightedLevDamMod()
    jaro_matching = JaroWinkler()

    matched_pair = (word_base, word_matched)

    pair_len = max(len(word_base), len(word_matched))
    wei_mod_similarity = 1 - (wei_mod_matching.distance(*matched_pair)/pair_len)

    phonetic_pairs = ((w1, w2) for w1 in phonetic_haase.encode_alpha(matched_pair[0]) for w2 in phonetic_haase.encode_alpha(matched_pair[1]))
    dam_phonetic_similarity = 0
    for ph_pair in phonetic_pairs:
        ph_pair_len = max(len(ph_pair[0]), len(ph_pair[1]))
        dam_phonetic_similarity = max(dam_phonetic_similarity, 1 - (damerau_matching.distance(*ph_pair)/ph_pair_len))

    ensemble_similarity = ensemble_sim(wei_mod_similarity, dam_phonetic_similarity)

    is_abbrev = (len(matched_pair[0])/len(matched_pair[1])) < ABBREVIATION_THRESHOLD
    final_similarity = jaro_matching.similarity(*matched_pair) if is_abbrev else ensemble_similarity

    return final_similarity > result_threshold, final_similarity

def entries_keywords(input_df: pd.DataFrame, input_df_col: str, kw_filename: str, horizon: int = 3) -> pd.Series:
    """Ekstrakcja keywordów i ich otoczenia dla każdego wpisu.

    Args:
        input_df (pd.DataFrame): Tabela z wpisami
        input_df_col (str): Kolumna z wpisami do ekstracji keywordów
        kw_filename (str): Ścieżka do pliku z keywordami w folderze data 
        horizon (int, optional): Szerokość otoczenia dla znalezionych keywordów. Defaults to 3.

    Returns:
        pd.Series: Kolumna z listą stringów dla każdego wpisu z input_df,
            jeśli brak dopasowań zwraca pustą listę.
    """

    with open(os.getenv("data_path") + "\\" + kw_filename, encoding="utf-8") as kw_file:
        base_keywords = kw_file.read().split()

    entries_matched = []

    for _, row in input_df.iterrows():
        entry_results = []
        entry = row[input_df_col]
        for word_idx, word in enumerate(entry.split()):
            for kw in base_keywords:
                if match_words(kw, word, result_threshold=0.8)[0] == True:
                    entry_results.append(
                        " ".join(entry.split()[max(0, word_idx - horizon) : word_idx + horizon + 1])
                    )
        entries_matched.append(entry_results)
    return pd.Series(entries_matched)

class WeightedLevDamMod():
    """Ważony algorytm Levenshtein-Damerau.
        Wagi:
            Brakujący znak - bez zmian,
            Nadmiarowy znak - waga błędu zmniejszona jeśli poprzedni znak był taki sam,
            Transpozycja sąsiednich znaków - waga zmniejszona,
            Zamiana znaku - waga zmniejszona jeśli brak znaku diakretycznego oraz kiedy sąsiad na klawiaturze.
    """

    SUBST_DICT_DIAC={
        ("ą","a"): 0.3,
        ("ć","c"): 0.3,
        ("ę","e"): 0.3,
        ("ł","l"): 0.3,
        ("ń","n"): 0.3,
        ("ó","o"): 0.3,
        ("ś","s"): 0.3,
        ("ź","z"): 0.3,
        ("ź","x"): 0.3,
        ("ż","z"): 0.3,
        ("ź","z"): 0.3,
        }

    SUBST_DICT_NEIGH = {
        'a': 'qwsz',
        'ą': 'qwszśź',
        'b': 'nghv',
        'c': 'vdfx',
        'ć': 'vdfx',
        'd': 'serfxc',
        'e': 'wrsd',
        'ę': 'wrsdś',
        'f': 'drtgcv',
        'g': 'ftyhvb',
        'h': 'gyujbn',
        'i': 'uojk',
        'j': 'huiknm',
        'k': 'jiolm',
        'l': 'kop',
        'ł': 'kopó',
        'm': 'jkn',
        'n': 'mhjb',
        'ń': 'mhjb',
        'o': 'ipkl',
        'ó': 'ipklł',
        'p': 'ol',
        'r': 'etdf',
        's': 'awedzx',
        'ś': 'awedzxąęż',
        't': 'ryfg',
        'u': 'yihj',
        'q': 'wa',
        'v': 'bfgc',
        'w': 'qeas',
        'x': 'csdz',
        'ź': 'csdzżść',
        'y': 'tugh',
        'z': 'xasąśź',
        'ż': 'xasąś',
    }

    @staticmethod
    def subst_cost_fn(char_base: str, char_matched: str):
        """Koszt dla zamiany znaku char_base na char_matched. """
        if (char_base, char_matched) in WeightedLevDamMod.SUBST_DICT_DIAC.keys():
            return WeightedLevDamMod.SUBST_DICT_DIAC[(char_base, char_matched)]
        elif char_base in WeightedLevDamMod.SUBST_DICT_NEIGH.keys() and char_matched in WeightedLevDamMod.SUBST_DICT_NEIGH[char_base]:
            return 0.6
        else: return 1
                
    @staticmethod
    def del_cost_fn(cur_char, prev_char):
        """Koszt usunięcia nadmiarowego znaku. cur_char - znak do wstawienia, prev_char - poprzedni znak"""
        if cur_char == prev_char:
            return 0.7
        else: return 1
        
    @staticmethod
    def ins_cos_fn():
        "Koszt wstawienia brakjącego znaku."
        return 1
    
    @staticmethod
    def trans_cost_fn(char_num):
        """Koszt transpozycji sąsiednich znaków. char_num - pozycja znaku w wyrazie"""
        return 1 if char_num == 1 else 0.4  # Sztywne 0.4 powodowało, że dla pierwszego znaku traktował wszystko jako transpozycja

    def distance(self, s0: str, s1: str) -> float:
        """Dystans pomiędzy s0 a s1, na podstawie ważonego Levenshtein-Damerau.

        Args:
            s0 (str): Wejściowy wyraz.
            s1 (str): Wejściowy wyraz.

        Raises:
            TypeError: None jako matchowane wyrazy.

        Returns:
            float: Dystans pomiędzy s0 a s1.
        """
        if s0 is None:
            raise TypeError("Argument s0 is NoneType.")
        if s1 is None:
            raise TypeError("Argument s1 is NoneType.")
        if s0 == s1:
            return 0.0
        inf = int(len(s0) + len(s1))
        da = dict()
        for i in range(len(s0)):
            da[s0[i]] = str(0)
        for i in range(len(s1)):
            da[s1[i]] = str(0)
        h = [[0] * (len(s1) + 2) for _ in range(len(s0) + 2)]
        for i in range(len(s0) + 1):
            h[i + 1][0] = inf
            h[i + 1][1] = i
        for j in range(len(s1) + 1):
            h[0][j + 1] = inf
            h[1][j + 1] = j
        for i in range(1, len(s0) + 1):
            db = 0
            for j in range(1, len(s1) + 1):
                i1 = int(da[s1[j - 1]])
                j1 = db

                cost = self.subst_cost_fn(s0[i - 1], s1[j - 1])
                if s0[i - 1] == s1[j - 1]:
                    cost = 0
                    db = j
                h[i + 1][j + 1] = min(h[i][j] + cost,
                                      h[i + 1][j] + self.del_cost_fn(s1[j - 1], s1[j - 2]),
                                      h[i][j + 1] + self.ins_cos_fn(),
                                      h[i1][j1] + (i - i1 - 1) + self.trans_cost_fn(i) + (j - j1 - 1))
            da[s0[i - 1]] = str(i)

        return h[len(s0) + 1][len(s1) + 1]


def get_keys_around(key):

    lines = "qwertyuiop", "asdfghjkl-", "-zxcvbnm--"
    try:
        line_index, index = [(i, l.find(key)) for i, l in enumerate(lines) if key in l][0]
    except:
        return ""
    lines = lines[line_index - 1 : line_index + 2] if line_index else lines[0:2]
    if line_index == 1:
        return [lines[1][index - 1]] + [
            line[index + i]
            for line in lines
            for i in [0, 1]
            if len(line) > index + i and line[index + i] != key and index + i >= 0
        ]
    elif line_index == 2:
        return [lines[1][index + 1]] + [
            line[index + i]
            for line in lines
            for i in [-1, 0]
            if len(line) > index + i and line[index + i] != key and index + i >= 0
        ]
    return [
        line[index + i]
        for line in lines
        for i in [-1, 0, 1]
        if len(line) > index + i and line[index + i] != key and index + i >= 0
    ]