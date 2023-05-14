#!/usr/bin/python3

import sys

# create an in-memory structure to keep track of the palindrome frequencies
palindrome_frequencies = {}

# receive palindrome words from stdin and update the palindrome frequencies
for word in sys.stdin:
    word = word.strip()
    palindrome_frequencies[word] = palindrome_frequencies.get(word, 0) + 1

# write the final palindrome frequencies to stdout
for palindrome, count in palindrome_frequencies.items():
        print(f"{palindrome}\t\t{count}")