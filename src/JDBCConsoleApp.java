import java.sql.*;
import java.util.Scanner;

public class JDBCConsoleApp {
    private static final String URL = "jdbc:postgresql://localhost:5432/SadikovDB";
    private static final String USER = "postgres";
    private static final String PASSWORD = "admin";

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver is not found. Include it in your library path");
        }
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("Choose an option:");
                System.out.println("1. Display all tables data");
                System.out.println("2. Execute predefined queries");
                System.out.println("3. Exit");
                int choice = scanner.nextInt();
                scanner.nextLine();

                switch (choice) {
                    case 1:
                        displayAllTablesData(connection);
                        break;
                    case 2:
                        executePredefinedQueries(connection, scanner);
                        break;
                    case 3:
                        System.out.println("Exiting...");
                        return;
                    default:
                        System.out.println("Invalid option. Please try again.");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void displayAllTablesData(Connection connection) throws SQLException {
        String[] tables = {"boards", "lists", "tasks", "labels", "users", "users_boards", "tasks_labels"};
        for (String table : tables) {
            System.out.println("Table: " + table);
            displayTableData(connection, table);
        }
    }

    private static void displayTableData(Connection connection, String tableName) throws SQLException {
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
                }
                System.out.println();
            }
        }
    }

    private static void executePredefinedQueries(Connection connection, Scanner scanner) throws SQLException {
        System.out.println("Choose a query to execute:");
        System.out.println("1. List all tasks with sorting and filtering");
        System.out.println("2. List unique users participating in boards");
        System.out.println("3. Count tasks in lists");
        System.out.println("4. List all tasks with labels");
        System.out.println("5. Find users participating in a specific board");
        System.out.println("6. Check if a user participates in a specific board");
        int choice = scanner.nextInt();
        scanner.nextLine();

        switch (choice) {
            case 1:
                listAllTasks(connection);
                break;
            case 2:
                listUniqueUsers(connection);
                break;
            case 3:
                countTasksInLists(connection);
                break;
            case 4:
                listTasksWithLabels(connection);
                break;
            case 5:
                findUsersInBoard(connection, scanner);
                break;
            case 6:
                checkUserInBoard(connection, scanner);
                break;
            default:
                System.out.println("Invalid option. Please try again.");
        }
    }

    private static void listAllTasks(Connection connection) throws SQLException {
        String query = "SELECT task_name, description FROM tasks WHERE description IS NOT NULL ORDER BY task_name ASC";
        executeAndPrintQuery(connection, query);
    }

    private static void listUniqueUsers(Connection connection) throws SQLException {
        String query = "SELECT DISTINCT u.username FROM users u JOIN users_boards ub ON u.id = ub.user_id";
        executeAndPrintQuery(connection, query);
    }

    private static void countTasksInLists(Connection connection) throws SQLException {
        String query = "SELECT boards.board_name, lists.list_name, COUNT(tasks.id) as task_count " +
                "FROM boards " +
                "JOIN lists ON boards.id = lists.board_id " +
                "LEFT JOIN tasks ON lists.id = tasks.list_id " +
                "GROUP BY boards.board_name, lists.list_name " +
                "ORDER BY boards.board_name, lists.list_name;";
        executeAndPrintQuery(connection, query);
    }

    private static void listTasksWithLabels(Connection connection) throws SQLException {
        String query = "SELECT t.task_name, string_agg(l.name, ', ') AS labels " +
                "FROM tasks t " +
                "JOIN tasks_labels tl ON t.id = tl.task_id " +
                "JOIN labels l ON tl.label_id = l.id " +
                "GROUP BY t.task_name";
        executeAndPrintQuery(connection, query);
    }

    private static void findUsersInBoard(Connection connection, Scanner scanner) throws SQLException {
        System.out.println("Enter board ID:");
        int boardId = scanner.nextInt();
        scanner.nextLine();
        String query = "SELECT username FROM users WHERE id IN (SELECT user_id FROM users_boards WHERE board_id = " + boardId + ")";
        executeAndPrintQuery(connection, query);
    }

    private static void checkUserInBoard(Connection connection, Scanner scanner) throws SQLException {
        System.out.println("Enter user ID:");
        int userId = scanner.nextInt();
        System.out.println("Enter board ID:");
        int boardId = scanner.nextInt();
        scanner.nextLine();
        String query = "SELECT EXISTS (SELECT 1 FROM users_boards WHERE user_id = " + userId + " AND board_id = " + boardId + ") AS user_in_board";
        executeAndPrintQuery(connection, query);
    }

    private static void executeAndPrintQuery(Connection connection, String query) throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ": " + columnValue);
                }
                System.out.println();
            }
        }
    }
}
