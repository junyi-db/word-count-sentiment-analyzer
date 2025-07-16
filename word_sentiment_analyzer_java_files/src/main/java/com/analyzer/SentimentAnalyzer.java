package com.analyzer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class SentimentAnalyzer {
    private Connection connection;
    private final String jdbcUrl;
    private final String token;
    
    /**
     * Initialize the sentiment analyzer with Databricks connection details.
     * 
     * @param serverHostname Your Databricks server hostname
     * @param httpPath The HTTP path for your Databricks SQL endpoint
     * @param personalAccessToken Your Databricks personal access token
     */
    public SentimentAnalyzer(String serverHostname, String httpPath, String personalAccessToken) {
        this.jdbcUrl = String.format(
            "jdbc:databricks://%s:443/default;transportMode=http;ssl=1;httpPath=%s;AuthMech=3",
            serverHostname, httpPath);
        this.token = personalAccessToken;
        
        try {
            // Load the Databricks JDBC driver
            Class.forName("com.databricks.client.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load Databricks JDBC driver: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Establishes a connection to Databricks if not already connected.
     */
    private void ensureConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("UID", "token");
            connectionProperties.setProperty("PWD", token);
            connection = DriverManager.getConnection(jdbcUrl, connectionProperties);
        }
    }
    
    /**
     * Analyzes the sentiment of text in a file.
     * 
     * @param filePath Path to the file containing text to analyze
     * @return The sentiment category directly from Databricks
     */
    public String analyzeSentiment(String filePath) throws IOException, SQLException {
        StringBuilder text = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                text.append(line).append("\n");
            }
        }
        
        if (text.length() == 0) {
            return "Neutral"; // Default if file is empty
        }
        
        ensureConnection();
        
        // Use Databricks' ai_analyze_sentiment() function
        String sql = "SELECT ai_analyze_sentiment(?) AS sentiment";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, text.toString());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // Get the sentiment directly as a string
                    String sentiment = rs.getString("sentiment");
                    return mapSentimentToCategory(sentiment);
                }
            }
        }
        
        return "Neutral"; // Default if analysis fails
    }
    
    /**
     * Maps the Databricks sentiment result to our sentiment categories.
     */
    private String mapSentimentToCategory(String sentiment) {
        if (sentiment == null) {
            return "Neutral";
        }
        
        // First letter capitalized, rest lowercase
        String formattedSentiment = sentiment.substring(0, 1).toUpperCase() + 
                                    sentiment.substring(1).toLowerCase();
        
        // Map "mixed" to "Neutral" if needed
        if (formattedSentiment.equalsIgnoreCase("mixed")) {
            return "Neutral";
        }
        
        return formattedSentiment;
    }
    
    /**
     * Closes the database connection.
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }
    }
}