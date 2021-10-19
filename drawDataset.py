from matplotlib import pyplot as plt
from collections import Counter

with open("dataset/AGES.txt", "r") as f:
    dataset = []
    for line in f.readlines():
        dataset.append(float(line.replace('\n', '')))
    plt.hist(dataset, bins=60)
    plt.xticks(range(20, 80)[::2])
    plt.show()

with open("dataset/HEIGHTS.txt", "r") as f:
    dataset = []
    for line in f.readlines():
        dataset.append(float(line.replace('\n', '')))
    plt.hist(dataset, bins=60)
    plt.xticks(range(140, 200)[::4])
    plt.show()