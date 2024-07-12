/**
 * This program connects to a MySQL database instance based on the location of a specified table,
 * retrieves the schema of the table, and displays it.
 *
 * The program prompts the user to input a table name, constructs a SQL query to select all records
 * from the specified table, and then extracts the table name from the SQL query.
 *
 * It then queries a local database to determine the location associated with the table. Based on
 * the location, the program connects to the appropriate Google Cloud Platform (GCP) MySQL instance.
 *
 * After establishing a connection, the program retrieves the schema of the table by executing a
 * DESCRIBE statement and prints the metadata information, such as column names, types, keys, and
 * any extra information.
 *
 * @authors Group-11
 */

package org.example;

import java.sql.*;
import java.util.Scanner;

public class GDC {
    private static final String GCP_DB_URL_INDIA = "jdbc:mysql://35.244.18.91:3306/vmysql1";
    private static final String GCP_DB_URL_USA = "jdbc:mysql://35.196.123.240:3306/vmysql2";
    private static final String GCP_DB_USER = "root";
    private static final String GCP_DB_PASSWORD = "root";

    /**
     * Main method of the program.
     *
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Input table name: ");
        String inpTable = sc.next();
        String sqlQuery = "SELECT * FROM "+inpTable;

        try {
            String tableName = extractTableName(sqlQuery);

            String location = getLocation(tableName);

            String gcpDBUrl = location.equals("India") ? GCP_DB_URL_INDIA : GCP_DB_URL_USA;
            Connection connection = DriverManager.getConnection(gcpDBUrl, GCP_DB_USER, GCP_DB_PASSWORD);
            System.out.println("Connected to GCP MySQL instance for location: " + location);
            String showSchema = "DESCRIBE "+tableName+";";
            PreparedStatement statement = connection.prepareStatement(showSchema);
            System.out.println(statement);
            ResultSet resultSet = statement.executeQuery();
            System.out.println(resultSet);
            System.out.println("Schema of table " + tableName + ":");
            while (resultSet.next()) {
                // Get column metadata
                String field = resultSet.getString(1); // Column name
                String type = resultSet.getString(2); // Column type
                String key = resultSet.getString(4); // Key (e.g., PRI for primary key)
                String extra = resultSet.getString(6); // Extra information

                // Print column metadata
                System.out.printf("%-20s %-20s %-10s %-20s\n", field, type, key, extra);
            }

            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Extracts the table name from the given SQL query.
     *
     * @param sqlQuery SQL query string
     * @return The table name extracted from the SQL query
     */
    private static String extractTableName(String sqlQuery) {
        return sqlQuery.split("\\s+")[3];
    }

    /**
     * Retrieves the location associated with the specified table from a local database.
     *
     * @param tableName Name of the table
     * @return The location associated with the table
     * @throws SQLException if a database access error occurs
     */
    private static String getLocation(String tableName) throws SQLException {
        String location = null;
        Connection connection = DriverManager.getConnection(GCP_DB_URL_USA, GCP_DB_USER, GCP_DB_PASSWORD);
        String query = "SELECT location_name FROM GDC WHERE table_name = ?;";
        System.out.println(query);
        PreparedStatement statement = connection.prepareStatement(query);
        System.out.println(statement);
        statement.setString(1, tableName);
        System.out.println(statement);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            location = resultSet.getString("location_name");
        }
        resultSet.close();
        statement.close();
        connection.close();
        System.out.println(location);
        return location;
    }
}
