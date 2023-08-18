import os
import numpy as np
from gensim.models import KeyedVectors

# def gimme_glove():
#     with open(os.getenv('glove_path'), encoding='utf8') as glove_raw:
#         for line in glove_raw.readlines():
#             splitted = line.split(' ')
#             yield splitted[0], np.array(splitted[1:], dtype=np.float)

# glove = {w: x for w, x in gimme_glove()}

# def closest_word(in_vector, glove=glove, top_n=1):
#     vectors = glove.values()
#     idx = np.argsort([np.linalg.norm(vec-in_vector) for vec in vectors])
#     return [glove.keys()[i] for i in idx[:top_n]]

# def text_to_vec(tokens):
#     words = [w for w in np.unique(tokens) if w in glove]
#     return np.array([glove[w] for w in words])


def glove_vectorize(tokens, glove):
    """Wektoryzacja przy użyciu glove

    Parameters:
    ----------
    tokens : str, list lub pd.Series'
        Wyraz/wyrazy do wektoryzacji
    glove : gensim.models
        Wczytany model glove

    Returns:
    -------
    vector_matrix : np.array
        Macierz wektorów dla unikalnych wyrazów w tokens
    """
    words = [w for w in np.unique(tokens) if w in glove.key_to_index.keys()]
    return np.array([glove.get_vector(w) for w in words])


def w2v_vectorize(tokens, word2vec):
    """Wektoryzacja przy użyciu word2vec

    Parameters:
    ----------
    tokens : str, list lub pd.Series'
        Wyraz/wyrazy do wektoryzacji
    word2vec : gensim.models
        Wczytany model word2vec

    Returns:
    -------
    vector_matrix : np.array
        Macierz wektorów dla unikalnych wyrazów w tokens
    """
    words = [w for w in np.unique(tokens) if w in word2vec.key_to_index.keys()]
    return np.array([word2vec.get_vector(w) for w in words])


def fasttext_vectorize(tokens, fasttext):
    """Wektoryzacja przy użyciu fasttext

    Parameters:
    ----------
    tokens : str, list lub pd.Series'
        Wyraz/wyrazy do wektoryzacji
    fasttext : gensim.models
        Wczytany model fasttext

    Returns:
    -------
    vector_matrix : np.array
        Macierz wektorów dla unikalnych wyrazów w tokens
    """
    words = [w for w in np.unique(tokens)]
    return np.array([fasttext.wv[w] for w in words])


def load_vector_models():
    """Wczytywanie modeli wektoryzujących do sesji.

    Returns:
    -------
    glove : gensim.models
        model wektorowy glove
    word2vec : gensim.models
        model wektorowy word2vec
    fasttext : gensim.models
        model wektorowy fasttext
    """
    glove = KeyedVectors.load_word2vec_format(
        os.getenv('data_path')+"\\glove_300_3_polish.txt")
    word2vec = KeyedVectors.load(
        os.getenv('data_path')+"\\word2vec_100_3_polish.bin")
    fasttext = KeyedVectors.load(
        os.getenv('data_path')+"\\fasttext_100_3_polish.bin")
    return glove, word2vec, fasttext


def load_fasttext():
    """Wczytywanie modeli wektoryzujących do sesji.

    Returns:
    -------
    fasttext : gensim.models
        model wektorowy fasttext
    """
    fasttext = KeyedVectors.load(
        os.getenv('data_path')+"\\fasttext_100_3_polish.bin")
    return fasttext
