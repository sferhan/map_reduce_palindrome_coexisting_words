#!/usr/bin/python3

from pyspark.sql import SparkSession
import json
import argparse


def is_palindrome(s: str):
    """
    Checks if the string @s is a palindrome.
    Returns False if @s is empty or non-palindrome, True otherwise
    """
    if not s:
        return False
    i = 0
    j = s.__len__()-1
    while i < j :
        if s[i] == ' ' or s[j] == ' ':
            return False
        if s[i] != s[j]:
            return False
        i, j = i+1, j-1
    return True

parser = argparse.ArgumentParser(description="Apache Spark script for finding all palindromes from amazon customer reviews data-set file")

parser.add_argument("--input", help="Path of the input file. File must be a from the Amazon Customer Review dataset(Each line a json field with a 'reviewText' string field)")
parser.add_argument("--output", help="Path of the output file.")

args = parser.parse_args()

# Create a SparkSession object
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the input file into an RDD
lines = spark.sparkContext.textFile(args.input)

palindrome_frequencies = {}

for line in lines.collect(): 
    line = line.strip()
    review = json.loads(line)

    # extract review text
    review_text : str = review['reviewText']
    review_text = review_text.strip()
    
    # extract words from the review text
    review_text_words = review_text.split()

    # check for palindromes and update palindrome_frequencies if palindrome is found
    for word in review_text_words:
        # ignore the . at the end of a sentence
        # but avoid ignoring a sequence of characters like ...
        if word.endswith(".") and not word.startswith("."):
            word = word[:-1]

        if is_palindrome(word):
            palindrome_frequencies[word] = palindrome_frequencies.get(word, 0) + 1

# write the final palindrome frequencies to a file on GCS
spark.sparkContext.parallelize(
    palindrome_frequencies.items()
).map(lambda x: f"{x[0]}\t\t{x[1]}").saveAsTextFile(args.output)

# Stop the SparkSession
spark.stop()