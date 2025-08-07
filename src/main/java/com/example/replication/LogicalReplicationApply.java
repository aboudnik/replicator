package com.example.replication;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.nio.ByteBuffer;
import java.math.BigDecimal;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LogicalReplicationApply {
    private static String USER;
    private static final String PASSWORD = "";
    private static final int RECONNECT_DELAY_MS = 5000; // 5 seconds delay between reconnect attempts
    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    public static void main(String[] args) throws Exception {
        // Validate command-line arguments
        if (args.length != 3) {
            System.err.println("Usage: java com.example.replication.LogicalReplicationApply <server_address> <source_db> <dest_db>");
            System.err.println("Example: java com.example.replication.LogicalReplicationApply localhost:5432 src dst");
            System.exit(1);
        }

        String serverAddress = args[0];
        String srcDb = args[1];
        String dstDb = args[2];
        String slotName = "slot_" + srcDb; // Generate slot name dynamically
        USER = "host_" + srcDb;

        // Construct JDBC URLs
        String srcUrl = "jdbc:postgresql://" + serverAddress + "/" + srcDb;
        String dstUrl = "jdbc:postgresql://" + serverAddress + "/" + dstDb;

        // JSON parser
        ObjectMapper mapper = new ObjectMapper();

        // Track LSN
        LogSequenceNumber lastAcknowledgedLSN = LogSequenceNumber.valueOf(0);

        while (true) {
            Connection srcConn = null;
            Connection dstConn = null;
            PGReplicationStream stream = null;

            try {
                // Set up source connection properties (replication)
                Properties srcProps = new Properties();
                PGProperty.USER.set(srcProps, USER);
                PGProperty.PASSWORD.set(srcProps, PASSWORD);
                PGProperty.ASSUME_MIN_SERVER_VERSION.set(srcProps, "9.4");
                PGProperty.REPLICATION.set(srcProps, "database");

                // Set up destination connection properties (standard)
                Properties dstProps = new Properties();
                dstProps.setProperty("user", USER);
                dstProps.setProperty("password", PASSWORD);

                // Establish connections
                srcConn = DriverManager.getConnection(srcUrl, srcProps);
                dstConn = DriverManager.getConnection(dstUrl, dstProps);
                dstConn.setAutoCommit(false); // Enable transaction control on dst

                PGConnection pgSrcConn = srcConn.unwrap(PGConnection.class);

                // Start replication stream from last acknowledged LSN
                stream = pgSrcConn.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName(slotName)
                        .withSlotOption("include-xids", "1")
                        .withStartPosition(lastAcknowledgedLSN)
                        .withStatusInterval(10, java.util.concurrent.TimeUnit.SECONDS)
                        .start();

                System.out.println("Connected and streaming from LSN: " + lastAcknowledgedLSN + " for source DB: " + srcDb + " using slot: " + slotName);

                // Process replication stream
                while (true) {
                    ByteBuffer msg = stream.readPending();
                    if (msg != null) {
                        byte[] bytes = new byte[msg.remaining()];
                        msg.get(bytes);
                        String message = new String(bytes);
                        System.out.println("Received WAL message: " + message);

                        // Parse JSON
                        JsonNode json = mapper.readTree(message);
                        JsonNode changes = json.get("change");

                        // Process all changes in the message
                        if (changes != null) {
                            List<JsonNode> messageChanges = new ArrayList<>();
                            for (JsonNode change : changes) {
                                String kind = change.get("kind").asText();
                                boolean isFromDestDb = false;
                                if (kind.equals("insert") || kind.equals("update")) {
                                    JsonNode columnNames = change.get("columnnames");
                                    JsonNode columnValues = change.get("columnvalues");
                                    for (int i = 0; i < columnNames.size(); i++) {
                                        if (columnNames.get(i).asText().equals("source_system")) {
                                            isFromDestDb = columnValues.get(i).asText().equals(dstDb);
                                            break;
                                        }
                                    }
                                }
                                if (!isFromDestDb) {
                                    messageChanges.add(change);
                                } else {
                                    System.out.println("Skipped change with source_system=" + dstDb + ": " + change);
                                }
                            }
                            // Apply all changes in a single transaction
                            if (!messageChanges.isEmpty()) {
                                applyMessageChanges(dstConn, messageChanges, srcDb);
                            }
                        }

                        // Acknowledge the received message
                        LogSequenceNumber currentLSN = stream.getLastReceiveLSN();
                        stream.setAppliedLSN(currentLSN);
                        stream.setFlushedLSN(currentLSN);
                        lastAcknowledgedLSN = currentLSN;
                        System.out.println("Acknowledged message at LSN: " + currentLSN);
                    }
                    Thread.sleep(100);
                }
            } catch (SQLException e) {
                System.err.println("SQL Error: " + e.getSQLState() + " - " + e.getMessage());
                e.printStackTrace();
                lastAcknowledgedLSN = handleReconnect(e, lastAcknowledgedLSN, serverAddress, srcDb, dstDb, slotName);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
                lastAcknowledgedLSN = handleReconnect(e, lastAcknowledgedLSN, serverAddress, srcDb, dstDb, slotName);
            } finally {
                // Clean up resources
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (SQLException e) {
                        System.err.println("Error closing stream: " + e.getMessage());
                    }
                }
                if (dstConn != null) {
                    try {
                        dstConn.rollback(); // Rollback any uncommitted changes
                        dstConn.close();
                    } catch (SQLException e) {
                        System.err.println("Error closing dstConn: " + e.getMessage());
                    }
                }
                if (srcConn != null) {
                    try {
                        srcConn.close();
                    } catch (SQLException e) {
                        System.err.println("Error closing srcConn: " + e.getMessage());
                    }
                }
            }
        }
    }

    private static LogSequenceNumber handleReconnect(Exception e, LogSequenceNumber lastAcknowledgedLSN, String serverAddress, String srcDb, String dstDb, String slotName) throws InterruptedException {
        System.out.println("Attempting to reconnect after error: " + e.getMessage());
        for (int attempt = 1; attempt <= MAX_RECONNECT_ATTEMPTS; attempt++) {
            try {
                Thread.sleep(RECONNECT_DELAY_MS);
                System.out.println("Reconnect attempt " + attempt + " of " + MAX_RECONNECT_ATTEMPTS);
                // Test connections
                Properties testProps = new Properties();
                testProps.setProperty("user", USER);
                testProps.setProperty("password", PASSWORD);
                String testSrcUrl = "jdbc:postgresql://" + serverAddress + "/" + srcDb;
                String testDstUrl = "jdbc:postgresql://" + serverAddress + "/" + dstDb;
                try (Connection testSrc = DriverManager.getConnection(testSrcUrl, testProps);
                     Connection testDst = DriverManager.getConnection(testDstUrl, testProps)) {
                    System.out.println("Reconnection successful, resuming from LSN: " + lastAcknowledgedLSN + " using slot: " + slotName);
                    return lastAcknowledgedLSN; // Return the last acknowledged LSN
                }
            } catch (SQLException ex) {
                System.err.println("Reconnect attempt " + attempt + " failed: " + ex.getMessage());
                if (attempt == MAX_RECONNECT_ATTEMPTS) {
                    System.err.println("Max reconnect attempts reached. Exiting.");
                    System.exit(1);
                }
            }
        }
        return lastAcknowledgedLSN; // Fallback return (unreachable due to System.exit)
    }

    private static void applyMessageChanges(Connection conn, List<JsonNode> changes, String srcDb) throws SQLException {
        try {
            // Apply all changes in a single transaction
            for (JsonNode change : changes) {
                String kind = change.get("kind").asText();
                switch (kind) {
                    case "insert":
                        applyInsert(conn, change, srcDb);
                        break;
                    case "update":
                        applyUpdate(conn, change, srcDb);
                        break;
                    case "delete":
                        applyDelete(conn, change);
                        break;
                }
            }
            conn.commit();
            System.out.println("Committed message with " + changes.size() + " changes");
        } catch (SQLException e) {
            System.err.println("Error applying message changes: " + e.getMessage());
            conn.rollback();
            throw e;
        }
    }

    private static void applyInsert(Connection conn, JsonNode change, String srcDb) throws SQLException {
        String schema = change.get("schema").asText();
        String table = change.get("table").asText();
        JsonNode columnNames = change.get("columnnames");
        JsonNode columnValues = change.get("columnvalues");
        JsonNode columnTypes = change.get("columntypes");
        StringBuilder sql = new StringBuilder("INSERT INTO " + schema + "." + table + " (");
        StringBuilder values = new StringBuilder(" VALUES (");

        int paramCount = 0;
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i).asText();
            if (!colName.equals("source_system")) {
                sql.append(colName);
                values.append("?");
                paramCount++;
                if (i < columnNames.size() - 1 && !columnNames.get(i + 1).asText().equals("source_system")) {
                    sql.append(", ");
                    values.append(", ");
                }
            }
        }
        // Include source_system in INSERT
        sql.append(", source_system");
        values.append(", ?");
        sql.append(")").append(values).append(")");
        sql.append(" ON CONFLICT DO NOTHING");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            int paramIndex = 1;
            for (int i = 0; i < columnValues.size(); i++) {
                if (!columnNames.get(i).asText().equals("source_system")) {
                    String colType = columnTypes.get(i).asText();
                    setPreparedStatementValue(stmt, paramIndex++, columnValues.get(i), colType);
                }
            }
            stmt.setString(paramIndex, srcDb); // Set source_system to source DB name
            stmt.executeUpdate();
        }
    }

    private static void applyUpdate(Connection conn, JsonNode change, String srcDb) throws SQLException {
        String schema = change.get("schema").asText();
        String table = change.get("table").asText();
        JsonNode columnNames = change.get("columnnames");
        JsonNode columnValues = change.get("columnvalues");
        JsonNode columnTypes = change.get("columntypes");
        JsonNode oldKeys = change.get("oldkeys");
        JsonNode oldKeyNames = oldKeys.get("keynames");
        JsonNode oldKeyValues = oldKeys.get("keyvalues");
        JsonNode oldKeyTypes = oldKeys.get("keytypes");

        StringBuilder sql = new StringBuilder("UPDATE " + schema + "." + table + " SET ");
        int valueCount = 0;
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i).asText();
            if (!colName.equals("source_system")) {
                sql.append(colName).append(" = ?");
                valueCount++;
                if (i < columnNames.size() - 1 && !columnNames.get(i + 1).asText().equals("source_system")) {
                    sql.append(", ");
                }
            }
        }
        // Set source_system
        sql.append(", source_system = ?");
        sql.append(" WHERE ");
        for (int i = 0; i < oldKeyNames.size(); i++) {
            sql.append(oldKeyNames.get(i).asText()).append(" = ?");
            if (i < oldKeyNames.size() - 1) {
                sql.append(" AND ");
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            int paramIndex = 1;
            // Set new values
            for (int i = 0; i < columnValues.size(); i++) {
                if (!columnNames.get(i).asText().equals("source_system")) {
                    String colType = columnTypes.get(i).asText();
                    setPreparedStatementValue(stmt, paramIndex++, columnValues.get(i), colType);
                }
            }
            stmt.setString(paramIndex++, srcDb); // Set source_system to source DB name
            // Set key values
            for (int i = 0; i < oldKeyValues.size(); i++) {
                String keyType = oldKeyTypes.get(i).asText();
                setPreparedStatementValue(stmt, paramIndex++, oldKeyValues.get(i), keyType);
            }
            stmt.executeUpdate();
        }
    }

    private static void applyDelete(Connection conn, JsonNode change) throws SQLException {
        String schema = change.get("schema").asText();
        String table = change.get("table").asText();
        JsonNode oldKeys = change.get("oldkeys");
        JsonNode oldKeyNames = oldKeys.get("keynames");
        JsonNode oldKeyValues = oldKeys.get("keyvalues");
        JsonNode oldKeyTypes = oldKeys.get("keytypes");

        StringBuilder sql = new StringBuilder("DELETE FROM " + schema + "." + table + " WHERE ");
        for (int i = 0; i < oldKeyNames.size(); i++) {
            sql.append(oldKeyNames.get(i).asText()).append(" = ?");
            if (i < oldKeyNames.size() - 1) {
                sql.append(" AND ");
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < oldKeyValues.size(); i++) {
                String keyType = oldKeyTypes.get(i).asText();
                setPreparedStatementValue(stmt, i + 1, oldKeyValues.get(i), keyType);
            }
            stmt.executeUpdate();
        }
    }

    private static void setPreparedStatementValue(PreparedStatement stmt, int paramIndex, JsonNode value, String pgType) throws SQLException {
        if (value.isNull()) {
            stmt.setObject(paramIndex, null);
            return;
        }

        switch (pgType.toLowerCase()) {
            case "int2":
                stmt.setShort(paramIndex, (short) value.asInt());
                break;
            case "int4":
            case "integer":
                stmt.setInt(paramIndex, value.asInt());
                break;
            case "int8":
                stmt.setLong(paramIndex, value.asLong());
                break;
            case "text":
            case "varchar":
            case "char":
                stmt.setString(paramIndex, value.asText());
                break;
            case "bool":
                stmt.setBoolean(paramIndex, value.asBoolean());
                break;
            case "numeric":
            case "decimal":
                stmt.setBigDecimal(paramIndex, new BigDecimal(value.asText()));
                break;
            case "float4":
                stmt.setFloat(paramIndex, Float.parseFloat(value.asText()));
                break;
            case "float8":
                stmt.setDouble(paramIndex, value.asDouble());
                break;
            case "timestamp":
            case "timestamptz":
                stmt.setTimestamp(paramIndex, Timestamp.valueOf(value.asText()));
                break;
            default:
                System.err.println("Warning: Unsupported PostgreSQL type '" + pgType + "' for parameter " + paramIndex + ", treating as String");
                stmt.setString(paramIndex, value.asText());
                break;
        }
    }
}