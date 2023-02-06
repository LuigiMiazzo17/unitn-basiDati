import java.sql.*;
import java.util.*;

//Author: Luigi Miazzo 226638
//Project: DataBase Assignment 3

public class Main {

    /*
     * used for exercise 3 and 4
     */
    static final int queryCount = 1000000;

    /*
     * used for DB connection
     */
    static final String DB_URI = "jdbc:postgresql://a.doma.in/db_name";
    static final String DB_USER = "db_user";
    static final String DB_PWD = "db_pwd";
    static Connection cachedConnection;

    /*
     * query for exercises
     */
    static final String query1 =
            "DROP TABLE IF EXISTS course; " +
                    "DROP TABLE IF EXISTS professor;";
    static final String query2 =
            "CREATE TABLE professor (" +
                    "id INT PRIMARY KEY NOT NULL, " +
                    "name VARCHAR(50) NOT NULL, " +
                    "address VARCHAR(50) NOT NULL, " +
                    "age INT NOT NULL, " +
                    "department FLOAT NOT NULL );" +
                    "CREATE TABLE course (" +
                    "cid VARCHAR(25) NOT NULL," +
                    "cname VARCHAR(50) NOT NULL," +
                    "credits VARCHAR(30) NOT NULL," +
                    "teacher INT NOT NULL," +
                    "FOREIGN KEY (teacher) REFERENCES professor (id) );";
    static final String query3 =
            "INSERT INTO professor VALUES(?, ?, ?, ?, ?);";
    static final String query4 =
            "INSERT INTO course VALUES (?, ?, ?, ?);";
    static final String query5 =
            "SELECT id FROM professor;";
    static final String query6 =
            "UPDATE professor " +
                    "SET department = 1973 " +
                    "WHERE department = 1940;";
    static final String query7 =
            "SELECT id, address FROM professor WHERE department = 1973;";
    static final String query8 =
            "CREATE INDEX b_tree ON professor (department);";
    static final String query9 =
            "SELECT id FROM professor;";
    static final String query10 =
            "UPDATE professor " +
                    "SET department = 1974 " +
                    "WHERE department = 1973;";
    static final String query11 =
            "SELECT id, address FROM professor WHERE department = 1974;";

    public static void main(String[] args) throws SQLException {

        Properties dbProperties = new Properties();
        dbProperties.setProperty("ssl", "false");
        dbProperties.setProperty("user", Main.DB_USER);
        dbProperties.setProperty("password", Main.DB_PWD);

        Main.cachedConnection = DriverManager.getConnection(Main.DB_URI, dbProperties);

        ResultSet currentResultSet;

        //exercise 1
        Main.exec_update(query1, 1);

        //exercise 2
        Main.exec_update(query2,2);

        //common for exercise 3 and 4
        HashSet<Integer> professorIDs = generateRandomIntegerList(queryCount);

        //exercise 3
        Main.exec_exercise_3(professorIDs);

        //exercise 4
        Main.exec_exercise_4(professorIDs);

        //exercise 5
        currentResultSet = Main.exec_query(query5, 5);
        while(currentResultSet.next())
            System.err.println(
                    currentResultSet.getString(1));

        //exercise 6
        Main.exec_update(query6, 6);

        //exercise 7
        currentResultSet = Main.exec_query(query7, 7);
        while(currentResultSet.next())
            System.err.println(
                    currentResultSet.getString(1) + "," +
                            currentResultSet.getString(2));

        //exercise 8
        Main.exec_update(query8, 8);

        //exercise 9
        currentResultSet = Main.exec_query(query9, 9);
        while(currentResultSet.next())
            System.err.println(
                    currentResultSet.getString(1));

        //exercise 10
        Main.exec_update(query10, 10);

        //exercise 11
        currentResultSet = Main.exec_query(query11, 11);
        while(currentResultSet.next())
            System.err.println(
                    currentResultSet.getString(1) + "," +
                            currentResultSet.getString(2));

        Main.cachedConnection.close();
    }

    /*
     * Generic functions for non prepared queries
     */
    private static void exec_exercise_3(HashSet<Integer> professorIDs) throws SQLException {
        Iterator<Integer> randomIDs = professorIDs.iterator();
        Iterator<String> randomNames = generateRandomStringList(queryCount, 20).iterator();
        Iterator<String> randomAddresses = generateRandomStringList(queryCount, 20).iterator();
        Iterator<Integer> randomAges = generateRandomIntegerList(queryCount, 18, 75).iterator();
        Iterator<Float> randomDepartments = generateRandomFloatList(queryCount).iterator();

        PreparedStatement _preparedStatement = Main.cachedConnection.prepareStatement(query3);

        for(int i = 0; i < Main.queryCount - 1; i++){
            _preparedStatement.setInt(1, randomIDs.next());
            _preparedStatement.setString(2, randomNames.next());
            _preparedStatement.setString(3, randomAddresses.next());
            _preparedStatement.setInt(4, randomAges.next());
            _preparedStatement.setFloat(5, randomDepartments.next());
            _preparedStatement.addBatch();
        }
        _preparedStatement.setInt(1, randomIDs.next());
        _preparedStatement.setString(2, randomNames.next());
        _preparedStatement.setString(3, randomAddresses.next());
        _preparedStatement.setInt(4, randomAges.next());
        _preparedStatement.setFloat(5, 1940);

        _preparedStatement.addBatch();
        long t0 = System.nanoTime();
        _preparedStatement.executeBatch();
        long t1 = System.nanoTime();

        System.out.println("Step 3 needs " + (t1 - t0) + " ns");
    }

    /*
     * Specific function for exercise 4
     */
    private static void exec_exercise_4(HashSet<Integer> professorIDs) throws SQLException {
        Iterator<String> randomIDs = generateRandomStringList(queryCount, 20).iterator();
        Iterator<String> randomCNames = generateRandomStringList(queryCount, 25).iterator();
        Iterator<String> randomCredits = generateRandomStringList(queryCount, 20).iterator();
        Iterator<Integer> teachers = professorIDs.iterator();

        PreparedStatement _preparedStatement = Main.cachedConnection.prepareStatement(query4);

        for(int i = 0; i < Main.queryCount; i++){
            _preparedStatement.setString(1, randomIDs.next());
            _preparedStatement.setString(2, randomCNames.next());
            _preparedStatement.setString(3, randomCredits.next());
            _preparedStatement.setInt(4, teachers.next());
            _preparedStatement.addBatch();
        }

        long t0 = System.nanoTime();
        _preparedStatement.executeBatch();
        long t1 = System.nanoTime();

        System.out.println("Step 4 needs " + (t1 - t0) + " ns");
    }

    /*
     * Generic functions for non prepared queries
     */
    private static void exec_update(String query, int queryNumber) throws SQLException {
        Statement _statement = Main.cachedConnection.createStatement();

        long t0 = System.nanoTime();
        _statement.executeUpdate(query);
        long t1 = System.nanoTime();

        System.out.println("Step "+ queryNumber + " needs " + (t1 - t0) + " ns");
    }
    private static ResultSet exec_query(String query, int queryNumber) throws SQLException {
        Statement _statement = Main.cachedConnection.createStatement();
        long t0 = System.nanoTime();
        ResultSet _res = _statement.executeQuery(query);
        long t1 = System.nanoTime();

        System.out.println("Step "+ queryNumber + " needs " + (t1 - t0) + " ns");
        return _res;
    }

    /*
     * Random generation helper functions for exercise 3 and 4
     */
    private static HashSet<Integer> generateRandomIntegerList(int len) {
        HashSet<Integer> set = new HashSet<>();
        while(set.size() < len)
            set.add((int) (Math.random() * 2147483647));
        return set;
    }
    private static HashSet<Float> generateRandomFloatList(int len) {
        HashSet<Float> set = new HashSet<>();
        while(set.size() < len)
            set.add((float) (Math.random() * 1900 ));
        return set;
    }
    private static ArrayList<Integer> generateRandomIntegerList(int len, int min, int max) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < len; i++)
            list.add((int) ((Math.random() * (max - min)) + min));
        return list;
    }
    private static ArrayList<String> generateRandomStringList(int count, int wordLen){
        ArrayList<String> list = new ArrayList<>();
        String validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int charsLen = validChars.length();
        for ( var i = 0; i < count; i++ ) {
            StringBuilder str = new StringBuilder();
            for(int j = 0; j < wordLen; j++)
                str.append(validChars.charAt((int) (Math.random() * charsLen)));
            list.add(str.toString());
        }
        return list;
    }
}
