package com.analyzer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.sql.SQLException;

public class Main {
    // Environment variable names for Databricks connection
    private static final String ENV_DATABRICKS_HOST = "DATABRICKS_HOST";
    private static final String ENV_DATABRICKS_HTTP_PATH = "DATABRICKS_HTTP_PATH";
    private static final String ENV_DATABRICKS_TOKEN = "DATABRICKS_TOKEN";
    
    public static void main(String[] args) {
        // Validate input
        if (args.length < 1) {
            System.err.println("Usage: java -jar word-sentiment-analyzer.jar <file-path>");
            return;
        }

        String filePath = args[0];
        SentimentAnalyzer sentimentAnalyzer = null;
        
        try {
            // Get Databricks connection details from environment variables
            String serverHostname = System.getenv(ENV_DATABRICKS_HOST);
            String httpPath = System.getenv(ENV_DATABRICKS_HTTP_PATH);
            String personalAccessToken = System.getenv(ENV_DATABRICKS_TOKEN);
            
            // Create sentiment analyzer with Databricks connection
            sentimentAnalyzer = new SentimentAnalyzer(serverHostname, httpPath, personalAccessToken);

            // Read the raw text from file
            String rawText = readRawText(filePath);
            
            // === Word Count Analysis ===
            // Use the manual word count method
            int totalCount = WordCounter.manualWordCount(rawText);
            System.out.println("Total word count: " + totalCount);

            // === Sentiment Analysis ===
            String sentimentCategory = sentimentAnalyzer.analyzeSentiment(filePath);

            // === Write CSV Summary ===
            String fileName = new File(filePath).getName().replace(".txt", "");
            String outputCsvPath = "/dbfs/tmp/" + fileName + "_text_analysis.csv";
            
            // Write the output as a properly formatted CSV
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputCsvPath))) {
                // Write header
                writer.write("file_name,raw_text,total_word_count,sentiment");
                writer.newLine();
                
                // Write data row
                writer.write(String.format(
                    "%s,%s,%d,%s",
                    escapeForCsv(fileName),
                    escapeForCsv(rawText),
                    totalCount,
                    escapeForCsv(sentimentCategory)
                ));
                writer.newLine();
            }

            System.out.println("✅ Analysis complete. CSV written to: " + outputCsvPath);

        } catch (IOException e) {
            System.err.println("❌ Error processing file: " + filePath);
            System.err.println("Error details: " + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("❌ Error connecting to Databricks: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("❌ Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Make sure to close the Databricks connection
            if (sentimentAnalyzer != null) {
                try {
                    sentimentAnalyzer.close();
                } catch (Exception e) {
                    System.err.println("⚠️ Warning: Error closing Databricks connection: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Reads the raw text from a file.
     * 
     * @param filePath Path to the file
     * @return The raw text content of the file
     * @throws IOException If there's an error reading the file
     */
    private static String readRawText(String filePath) throws IOException {
        StringBuilder text = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                text.append(line).append("\n");
            }
        }
        return text.toString();
    }
    
    /**
     * Escapes a string for CSV format following RFC 4180 standard.
     * 
     * @param text The text to escape
     * @return The escaped text
     */
    private static String escapeForCsv(String text) {
        if (text == null) {
            return "\"\"";  // Empty quoted field
        }
        
        // Replace double quotes with two double quotes (CSV standard)
        String escaped = text.replace("\"", "\"\"");
        
        // Always wrap in quotes to handle any special characters
        return "\"" + escaped + "\"";
    }
}