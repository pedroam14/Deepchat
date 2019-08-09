import json
import random
import tensorflow as tf
import tflearn as tfl
import numpy as np
import nltk
from nltk.stem.lancaster import LancasterStemmer
stemmer = LancasterStemmer()

if __name__ == "__main__":
    with open("data.json") as file:
        data = json.load(file)

    words:  list = []
    labels: list = []
    docsX:  list = []
    docsY:  list = []

    for intent in data["intents"]:
        for pattern in intent["patterns"]:
            wrds = nltk.word_tokenize(pattern)
            words.extend(wrds)
            docsX.append(wrds)
            docsY.append(intent["tag"])

        if intent["tag"] not in labels:
            labels.append(intent["tag"])

    words = [stemmer.stem(w.lower()) for w in words if w != "?"]
    words = sorted(list(set(words)))

    labels = sorted(labels)

    training = []
    output = []

    out_empty = [0 for _ in range(len(labels))]

    for x, doc in enumerate(docsX):
        bag = []

        wrds = [stemmer.stem(w.lower()) for w in doc]

        for w in words:
            if w in wrds:
                bag.append(1)
            else:
                bag.append(0)

        outputRow = out_empty[:]
        outputRow[labels.index(docsY[x])] = 1

        training.append(bag)
        output.append(outputRow)

    training = np.array(training)
    output = np.array(output)
    tf.reset_default_graph()

    net = tfl.input_data(shape=[None, len(training[0])])
    net = tfl.fully_connected(net, 16)
    net = tfl.fully_connected(net, 16)
    net = tfl.fully_connected(net, len(output[0]), activation="softmax")
    net = tfl.regression(net)

    model = tfl.DNN(net)
    model.fit(training, output, n_epoch=5000, batch_size=16, show_metric=True)
    model.save("Model/model.tflearn")
