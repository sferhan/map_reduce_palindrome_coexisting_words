#!/usr/bin/python3

from pyspark.sql import SparkSession
import json


def get_cooccuring_words_in_sentence(sentence: str):
    """
    Returns a list of all co-occuring words in a sentence
    A co-occurring word pair is a pair of words that occur in the same sentence
    """
    words = sentence.split()
    cleaned_words = []
    
    for word in words:
        # remove special characters from the word
        cleaned_word = ''.join(e for e in word if e.isalnum())
        cleaned_words.append(cleaned_word)
    
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

# Create a SparkSession object
spark = SparkSession.builder.appName("CoOccuringWordCount").getOrCreate()

# Read the input file into an RDD
lines = spark.sparkContext.textFile("gs://cc-mapreduce-project/dataset/sample.txt")

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
).map(lambda x: f"{x[0]}\t\t{x[1]}").saveAsTextFile("gs://cc-mapreduce-project/output/spark_cooccuring_words.txt")

# Stop the SparkSession
spark.stop()