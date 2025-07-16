package com.analyzer;

public class WordCounter {
    /**
     * Manual word count for verification - counts words as separated by whitespace
     * after removing punctuation, but treats hyphenated words as single words.
     */
    public static int manualWordCount(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }
        
        // Replace punctuation with spaces, but preserve hyphens and apostrophes
        String processed = text.replaceAll("[\\p{Punct}&&[^'-]]", " ");
        String[] words = processed.trim().split("\\s+");
        
        return words.length;
    }
}