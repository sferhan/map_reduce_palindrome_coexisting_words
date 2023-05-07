#!/usr/bin/env python

import sys
import json

def is_palindrome(s: str):
    if not s or s == ".":
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


for line in sys.stdin: 
    line = line.strip()
    review = json.loads(line)
    
    # extract review text
    review_text : str= review['reviewText']
    review_text = review_text.strip()
    
    # extract words from the review text
    review_text_words = review_text_words.split(review_text)
    for word in review_text_words:
        if is_palindrome(review_text):
            print(word)
