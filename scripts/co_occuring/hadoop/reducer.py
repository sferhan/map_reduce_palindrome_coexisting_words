#!/usr/bin/python3

import sys

# create an in-memory structure to keep track of the palindrome frequencies
cooccuring_frequencies = {}

# receive palindrome words from stdin and update the palindrome frequencies
for word_pair_str in sys.stdin:
    word_pair_str = word_pair_str.strip()
    word1, word2 = word_pair_str.split(',')
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

# write the final palindrome frequencies to stdout
for word_pair, count in cooccuring_frequencies.items():
        print(f"{word_pair}\t\t{count}")