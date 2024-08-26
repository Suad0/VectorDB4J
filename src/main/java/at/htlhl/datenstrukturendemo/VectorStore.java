package at.htlhl.datenstrukturendemo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VectorStore {

    private Connection conn;
    private ObjectMapper objectMapper = new ObjectMapper();

    public VectorStore(String dbPath) throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        initializeSchema();
    }

    private void initializeSchema() throws SQLException {
        // Drop the table if it exists to ensure we start fresh
        String dropTable = "DROP TABLE IF EXISTS documents";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(dropTable);
        }

        // Create the table with the UNIQUE constraint on 'text'
        String createTable = "CREATE TABLE IF NOT EXISTS documents (" +
                "id INTEGER PRIMARY KEY," +
                "text TEXT NOT NULL UNIQUE," +  // Ensure text is UNIQUE
                "vector BLOB NOT NULL)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
        }
    }



    public void upsertDocument(String doc) throws SQLException, IOException {
        List<Double> vector = simpleEncode(doc);
        byte[] vectorBlob = serializeVector(vector);

        String insert = "INSERT INTO documents (text, vector) VALUES (?, ?) " +
                "ON CONFLICT(text) DO UPDATE SET vector = excluded.vector";
        try (PreparedStatement pstmt = conn.prepareStatement(insert)) {
            pstmt.setString(1, doc);
            pstmt.setBytes(2, vectorBlob);
            pstmt.executeUpdate();
        }
    }

    private List<Double> simpleEncode(String doc) {
        List<Double> vector = new ArrayList<>(Collections.nCopies(26, 0.0));
        for (char ch : doc.toLowerCase().toCharArray()) {
            if (Character.isLetter(ch)) {
                int idx = ch - 'a';
                vector.set(idx, vector.get(idx) + 1.0);
            }
        }
        return vector;
    }

    private byte[] serializeVector(List<Double> vector) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            objectMapper.writeValue(baos, vector);
            return baos.toByteArray();
        }
    }

    private List<Double> deserializeVector(byte[] vectorBlob) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(vectorBlob)) {
            return objectMapper.readValue(bais, new TypeReference<List<Double>>() {
            });
        }
    }

    public List<Map.Entry<String, Double>> getTopN(String query, int n) throws SQLException, IOException {
        List<Double> embeddedQuery = simpleEncode(query);
        List<Map.Entry<String, Double>> scores = new ArrayList<>();

        String sql = "SELECT text, vector FROM documents";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                String text = rs.getString("text");
                byte[] vectorBlob = rs.getBytes("vector");
                List<Double> vector = deserializeVector(vectorBlob);
                double similarity = cosineSimilarity(embeddedQuery, vector);
                scores.add(Map.entry(text, similarity));
            }
        }

        return scores.stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .limit(n)
                .collect(Collectors.toList());
    }

    private double cosineSimilarity(List<Double> u, List<Double> v) {
        double dotProduct = 0.0;
        double normU = 0.0;
        double normV = 0.0;

        for (int i = 0; i < u.size(); i++) {
            dotProduct += u.get(i) * v.get(i);
            normU += u.get(i) * u.get(i);
            normV += v.get(i) * v.get(i);
        }

        return dotProduct / (Math.sqrt(normU) * Math.sqrt(normV));
    }

    public static void main(String[] args) {
        try {
            String dbPath = "vector_store.db";
            VectorStore store = new VectorStore(dbPath);

            store.upsertDocument("I like apples");
            store.upsertDocument("I like pears");
            store.upsertDocument("I like dogs");
            store.upsertDocument("I like cats");

            List<Map.Entry<String, Double>> topDocs = store.getTopN("I like apples", 1);
            System.out.println(topDocs);

            List<Map.Entry<String, Double>> topDocs2 = store.getTopN("animal", 2);
            System.out.println(topDocs2);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}



