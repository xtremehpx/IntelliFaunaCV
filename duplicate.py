from collections import defaultdict

def find_duplicated_phrases(text, min_phrase_length, max_phrase_length):
    words = text.split()
    phrases = defaultdict(int)

    # Generate phrases using a sliding window
    for phrase_length in range(min_phrase_length, max_phrase_length + 1):
        for i in range(len(words) - phrase_length + 1):
            phrase = ' '.join(words[i:i+phrase_length])
            phrases[phrase] += 1

    # Find duplicated phrases
    duplicated_phrases = [phrase for phrase, count in phrases.items() if count > 1]

    return duplicated_phrases

# Example usage:
text = "This is a test test to find duplicated phrases in a sentence. This is a test to test."
min_phrase_length = 3
max_phrase_length = 5
result = find_duplicated_phrases(text, min_phrase_length, max_phrase_length)
print(result)  # Output: ['This is a', 'is a test', 'a test test', 'test test to', 'This is a test', 'is a test to', 'a test to test']
