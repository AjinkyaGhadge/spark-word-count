'''
    This Spark Program counts total number of words in a file.
'''

from pyspark import SparkContext

class WordCount:
    def __init__(self, name):
        self.spark = SparkContext('local[3]', name)

    def read_RDD(self, path):
        return self.spark.textFile(path)

    def count_words(self, lines):
        return lines.filter(lambda x: len(x)).flatMap(lambda x: x.split()).count()

if __name__ == '__main__':
    rdd = WordCount('wordcount')
    lines = rdd.read_RDD('/path/to/file/CountWords.txt')
    word_count = rdd.count_words(lines)
    print(word_count)
