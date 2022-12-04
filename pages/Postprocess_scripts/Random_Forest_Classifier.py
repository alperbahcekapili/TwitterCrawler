
import numpy as np
import re
import nltk
from sklearn.datasets import load_files
nltk.download('stopwords')
import pickle
from nltk.corpus import stopwords

class RFClassifier:
    # will take inputs as follows
    # list({id: "123" , document: "This is tweet example"}, ...)

    def preprocess_texts(self, X):
        documents = []

        from nltk.stem import WordNetLemmatizer
        stemmer = WordNetLemmatizer()

        for sen in range(0, len(X)):
            # Remove all the special characters
            document = re.sub(r'\W', ' ', str(X[sen]))
            
            # remove all single characters
            document = re.sub(r'\s+[a-zA-Z]\s+', ' ', document)
            
            # Remove single characters from the start
            document = re.sub(r'\^[a-zA-Z]\s+', ' ', document) 
            
            # Substituting multiple spaces with single space
            document = re.sub(r'\s+', ' ', document, flags=re.I)
            
            # Removing prefixed 'b'
            document = re.sub(r'^b\s+', '', document)
            
            # Converting to Lowercase
            document = document.lower()
            
            # Lemmatization
            document = document.split()

            document = [stemmer.lemmatize(word) for word in document]
            document = ' '.join(document)
            
            documents.append(document)

        from sklearn.feature_extraction.text import TfidfVectorizer
        tfidfconverter = TfidfVectorizer(max_features=26,  stop_words=stopwords.words('turkish'))
        X = tfidfconverter.fit_transform(documents).toarray()

        return X
    
    def predict(self, X, classifier):
        if type(X[0]) is str:
            
            X = self.preprocess_texts(X)
        y_pred = classifier.predict(X)
        return y_pred


    def __init__(self, model_path, inqueue, outqueue):
        self.inqueue = inqueue
        self.outqueue = outqueue

        with open(model_path, 'rb') as training_model:
            model = pickle.load(training_model)

        while not self.inqueue.empty():
            data = self.inqueue.get(timeout=1)
            X = list(data.Text)
        
            ypreds = self.predict(X, model)
            data["Stance"] = ypreds
            
            self.outqueue.put({"Stances_df":data, "break":True})