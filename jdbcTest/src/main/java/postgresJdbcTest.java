import java.sql.*;

public class postgresJdbcTest {
    public static void main(String[] args) {
        try {
            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/hackafest", "saman",
                    "saman");
            String sql = "SELECT * FROM cotiviti.test";
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
