
from sklearn.metrics import accuracy_score
from sklearn.neighbors import KNeighborsClassifier


class KNN_manhattan:
    """Klasa trenująca i modelująca zbiór danych za pomocą algorytmu kNN z użyciem metryki Manhattan.

    Attributes
    -----------
    n : int
    Liczba sąsiadów ustalona dla algorytmu kNN

    x: np.array
    Zmienne niezależne dla przeprocesowanego zbioru danych

    y: np.array
    Zmienna zależna dla przeprocesowanego zbioru danych

    Methods
    -------

    fit(self, x, y) - metoda trenująca zmienne zależne oraz zmienną niezależną

    predict(self, x, classifier) - metoda tworząca predykcje dla wytrenowanych zmiennych przy użyciu algorytmu KNN

    score(self, target_pred, y_test) - metoda wyliczająca metrykę accuracy między zbiorem testowym a predykcją

    """

    def __init__(self, n):
        """__init__

        Parameters
        ----------
        n : int
            Parametr dla algorytmu kNN oznaczający liczbę sąsiadów zadaną w podanym algorytmie.
        """
        self.neighbors = n
        self.features = []
        self.target = []

    def fit(self, x, y):
        """Funkcja zwracająca wytrenowany model dla podanych danych

        Parameters
        ----------
        x : np.array
            Zmienne niezależne dla zbioru danych użytego w modelowaniu
        y : np.array
            Zmienna zależna dla zbioru danych użytych w modelowaniu

        Returns
        -------
        sklearn.neighbors._classification.KNeighborsClassifier
            Zwraca wytrenowany model klasyfikacyjny metodą kNN
        """
        self.features = x.values.tolist()
        self.target = y.values.tolist()

        classifier = KNeighborsClassifier(
            n_neighbors=self.neighbors, metric='manhattan')

        classifier.fit(self.features, self.target)

        return classifier

    def predict(self, x, classifier):
        """Funkcja tworząca predykcję dla wytrenowanego modelu klasyfikacyjnego

        Parameters
        ----------
        x : np.array
            Zmienne niezależne dla zbioru danych użytego w modelowaniu
        classifier : sklearn.neighbors._classification.KNeighborsClassifier
            Model klasyfikacyjny kNN dla metryki Manhattan

        Returns
        -------
        np.array
            Funkcja zwraca predykcję modelu dla zbioru testowego
        """

        target_pred = classifier.predict(x)

        return target_pred

    def score(self, target_pred, y_test):
        """Funkcja licząca metrykę accuracy dla zbioru testowego

        Parameters
        ----------
        target_pred : np.array
            Predykcja dla zbioru testowego
        y_test : np.array
            Rzeczywisty zbiór testowy

        Returns
        -------
        int
            Funkcja zwraca wynik metryki accuracy (dla zbioru testowego)
        """

        acc = accuracy_score(y_test, target_pred)

        print("Accuracy score for target prediction is {}".format(acc))

        return acc
