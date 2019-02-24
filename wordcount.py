from pyspark import SparkContext

class RDD:
    def __init__(self, name):
        self.spark = SparkContext('local[3]', name)

    def readRDD(self, path):
        return self.spark.textFile(path)

    def count_words(self, lines):
        return lines.filter(lambda x: len(x)).flatMap(lambda x: x.split()).count()

if __name__ == '__main__':
    rdd = RDD('wordcount')
    lines = rdd.readRDD('/path/to/file/CountWords.txt')
    word_count = rdd.count_words(lines)
    print(word_count)
