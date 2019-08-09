import json
import random as rnd
import numpy as np
import tensorflow as tf
import tflearn as tfl
import nltk
from nltk.stem.lancaster import LancasterStemmer
stemmer = LancasterStemmer()

if (__name__ == "__main__"):
    with open('data.json') as file:
        data = json.load(file)

    words: list = []
    labels: list = []
    docsX: list = []
    docsY: list = []
    for intent in data['intents']:
        for pattern in intent['patterns']:
            wrds: list = nltk.word_tokenize(pattern)
            words.extend(wrds)
            docsX.append(wrds)
            docsY.append(intent["tag"])
        if(intent['tag'] not in labels):
            labels.append(intent['tag'])
    '''
    for i in range(len(words)):
        print(words[i])
    # '''
    j = 0
    # method in charge of updating the list of words to just their stems, and removing question marks
    for w in words:
        if (w != "?"):
            words[j] = stemmer.stem(w.lower())
            j += 1
    for i in range(len(words)-1, j, -1):
        words.pop(i)
    words = sorted(list(set(words)))
    '''
    for i in range(len(words)):
        print(words[i])
    #'''
    labels = sorted(labels)

    training: list = []
    output: list = []

    outEmpty = [0 for i in range(len(labels))]
    for x, doc in enumerate(docsX):
        bag: list = []
        wrds = [stemmer.stem(w.lower()) for w in doc]

        for w in words:
            if w in wrds:
                bag.append(1)
            else:
                bag.append(0)
        outputRow = outEmpty[:]
        outputRow[labels.index(docsY[x])] = 1

        training.append(bag)
        training.append(outputRow)
    training = np.array(training)
    output = np.array(outputRow)
