'''
  This Spark Program counts each word in file and display the word counts.
'''

from pyspark import SparkContext

class WordCountByValue:
    def __init__(self, name):
        self.spark = SparkContext('local[3]', name)

    def read_RDD(self, path):
        return self.spark.textFile(path)

    def count_words(self, lines):
        return lines.filter(lambda x: len(x)).flatMap(lambda x: x.split()).countByValue()

if __name__ == '__main__':
    rdd = WordCountByValue('wordcount')
    lines = rdd.read_RDD('C:/Users/Austin Joyal/Documents/Spark Text files/CountWords.txt')
    word_count = rdd.count_words(lines)
    print('\n'.join(f'{word}: {count}' for word, count in word_count.items()))
