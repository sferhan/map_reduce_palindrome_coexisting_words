#!/usr/bin/python3

from pyspark.sql import SparkSession
import json
import argparse


def get_cooccuring_words_in_sentence(sentence: str):
    """
    Returns a list of all co-occuring words in a sentence
    A co-occurring word pair is a pair of words that occur in the same sentence
    """
    words = sentence.split()
    cleaned_words = []
    
    for word in words:
        # ignore the . at the end of a sentence
        if word.endswith("."):
            word = word[:-1]

        alphabets_only = True
        for c in word:
            if not c.isalpha():
                alphabets_only = False
        
        # word must be all alphabets
        if word and alphabets_only:
            cleaned_words.append(word)
    
    co_occuring = []
    
    for i in range(cleaned_words.__len__()):
        word1 = cleaned_words[i]
        
        if not word1:
            continue
        
        for j in range(i+1, cleaned_words.__len__()):
            word2 = cleaned_words[j]
            
            if not word2:
                continue
            
            co_occuring.append((word1, word2))
    return co_occuring


parser = argparse.ArgumentParser(description="Apache Spark script for finding all co-occuring words from amazon customer reviews data-set file")

parser.add_argument("input", help="Path of the input file. File must be a from the Amazon Customer Review dataset(Each line a json field with a 'reviewText' string field)")
parser.add_argument("output", help="Path of the output file.")

args = parser.parse_args()

# Create a SparkSession object
spark = SparkSession.builder.appName("CoOccuringWordCount").getOrCreate()

# Read the input file into an RDD
lines = spark.sparkContext.textFile(args.input)

cooccuring_frequencies = {}

for line in lines.collect(): 
    line = line.strip()
    review = json.loads(line)

    # extract review text
    review_text : str = review['reviewText']
    review_text = review_text.strip()
    
    # extract sentences from the review text
    review_text_sentences = review_text.split('.')

    for sentence in review_text_sentences:
        co_occuring_word_pairs = get_cooccuring_words_in_sentence(sentence)
        for word1, word2 in co_occuring_word_pairs:
            word1_ci, word2_ci = word1.lower(), word2.lower()
            key = f"{word1_ci}, {word2_ci}"

            if key not in cooccuring_frequencies:
                reverse_key = f"{word2_ci},{word1_ci}"
                if reverse_key not in cooccuring_frequencies:
                        cooccuring_frequencies[key] = 1
                else:
                        cooccuring_frequencies[reverse_key] = cooccuring_frequencies[reverse_key] + 1
            else:
                cooccuring_frequencies[key] = cooccuring_frequencies[key] + 1

# write the final palindrome frequencies to a file on GCS
spark.sparkContext.parallelize(
    cooccuring_frequencies.items()
).map(lambda x: f"{x[0]}\t\t{x[1]}").saveAsTextFile(args.output)

# Stop the SparkSession
spark.stop()