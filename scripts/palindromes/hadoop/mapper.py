#!/usr/bin/python3

import sys
import json


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

for line in sys.stdin: 
    line = line.strip()
    review = json.loads(line)

    # extract review text
    review_text : str = review['reviewText']
    review_text = review_text.strip()
    
    # extract words from the review text
    review_text_words = review_text.split()

    # check for palindromes and write to stdout if palindrome is found
    for word in review_text_words:
        # remove special characters from the word
        cleaned_word = ''.join(e for e in word if e.isalnum())

        if is_palindrome(cleaned_word):
            print(cleaned_word)
