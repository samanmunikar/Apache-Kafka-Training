import java.sql.*;

public class MySQLJdbcTest {
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/cotiviti", "root",
                    "cloudera");
            String sql = "SELECT * FROM test";
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
