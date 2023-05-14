#!/usr/bin/python3

import sys
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


for line in sys.stdin: 
    line = line.strip()
    review = json.loads(line)

    # extract review text
    review_text : str = review['reviewText']
    review_text = review_text.strip()
    
    # extract sentences from the review text
    review_text_sentences = review_text.split('.')

    for sentence in review_text_sentences:
        co_occuring_words = get_cooccuring_words_in_sentence(sentence)

        for word_pair in co_occuring_words:
            print(f"{word_pair[0]},{word_pair[1]}")
