package com.codegym.dao;

import com.codegym.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDao implements IUserDAO{
    //Kết nối DB
    private static final String URLJDBC = "jdbc:mysql://localhost:3306/demoJDBC";
    private static final String USERJDBC = "root";
    private static final String PASSWORDJDBC = "12345678";

    //Câu lệnh querry
    private static final String INSERT_USERS_SQL = "INSERT INTO users (name, email, country) VALUES (?, ?, ?);";
    private static final String SELECT_USER_BY_ID = "SELECT id,name,email,country FROM users WHERE id =?";
    private static final String SELECT_All_USERS = "SELECT * FROM users";
    private static final String DELETE_USERS_SQL = "DELETE FROM users WHERE id = ?;";
    private static final String UPDATE_USERS_SQL = "UPDATE users SET name = ?,email= ?, country =? WHERE id = ?;";
    public static final String GET_USER_BY_ID_PROCEDURE = "{CALL get_user_by_id(?)}";
    public static final String CALL_INSERT_USER_PROCEDURE = "{CALL insert_user(?,?,?)}";
    public static final String INSERT_USER_PERMISSION_SQL = "INSERT INTO user_permision(userId, permisionId) VALUES(?,?)";

    public UserDao(){
    }

    //Lấy kết nối với CSDL
    protected Connection getConnection() {
        Connection connection = null;
        try {
            //Driver
            Class.forName("com.mysql.jdbc.Driver");
            //Connection
            connection = DriverManager.getConnection(URLJDBC, USERJDBC, PASSWORDJDBC);
            System.out.println("Ket noi thanh cong");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Ket noi chua thanh cong");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    @Override
    public void insertUser(User user) throws SQLException {
        System.out.println(SELECT_All_USERS);
        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_USERS_SQL);
        ){
            preparedStatement.setString(1, user.getName());
            preparedStatement.setString(2, user.getEmail());
            preparedStatement.setString(3, user.getCountry());
            System.out.println(preparedStatement);
            preparedStatement.executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @Override
    public User selectUser(int id) {
        User user = null;
        try(
                Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(SELECT_USER_BY_ID);
                ) {
            preparedStatement.setInt(1, id);
            System.out.println(preparedStatement);
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()){
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("country");
                user = new User(id, name, email, country);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public List<User> selectAllUsers() {
        List<User> users = new ArrayList<>();
        try(
                //Lấy kết nối đến CSDL
                Connection connection = getConnection();
                //Chuẩn bị câu lệnh truy vấn
                PreparedStatement preparedStatement = connection.prepareStatement(SELECT_All_USERS);
                //Lưu dữ liệu thực thi câu lệnh vào trong ResultSet
//                ResultSet rs = preparedStatement.executeQuery();
        ) {
            System.out.println(preparedStatement);
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()){
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("country");
                users.add( new User(id, name, email, country));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    @Override
    public boolean deleteUser(int id) throws SQLException {
        boolean rowDeleted;
        try (
                Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(DELETE_USERS_SQL);) {
            preparedStatement.setInt(1, id);
            rowDeleted = preparedStatement.executeUpdate() > 0;
        }
        return rowDeleted;
    }

    @Override
    public boolean updateUser(User user) throws SQLException {
        boolean rowUpdate;
        try (
                Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_USERS_SQL);) {
            preparedStatement.setString(1, user.getName());
            preparedStatement.setString(2, user.getEmail());
            preparedStatement.setString(3, user.getCountry());
            preparedStatement.setInt(4, user.getId());

            rowUpdate = preparedStatement.executeUpdate() > 0;
        }
        return rowUpdate;
    }

    private void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }

    @Override
    public User getUserById(int id) {
        User user = null;
        try(
                Connection connection = getConnection();
                CallableStatement callableStatement = connection.prepareCall(GET_USER_BY_ID_PROCEDURE);
                ){
            callableStatement.setInt(1, id);
            ResultSet rs = callableStatement.executeQuery();
            while (rs.next()){
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("email");
                user = new User(id,name,email,country);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void insertUserStore(User user) throws SQLException {
        try (
                Connection connection = getConnection();
                CallableStatement callableStatement = connection.prepareCall(CALL_INSERT_USER_PROCEDURE);
                ) {
            callableStatement.setString(1, user.getName());
            callableStatement.setString(2, user.getEmail());
            callableStatement.setString(3,user.getCountry());
            System.out.println(callableStatement);
            callableStatement.executeQuery();
        }catch (SQLException e){
            printSQLException(e);
        }
    }

    @Override
    public void addUserTransaction(User user, int[] permisions) {
        Connection connection = null;
        //để thêm user
        PreparedStatement pstmt = null;
        // để chỉ định permission cho người dùng
        PreparedStatement pstmtAssignment = null;
        //để lấy user_id
        ResultSet rs = null;
        try {
           connection = getConnection();
           //set auto commit to false
           connection.setAutoCommit(false);
           //insert user
            pstmt = connection.prepareStatement(INSERT_USERS_SQL, Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, user.getName());
            pstmt.setString(2, user.getEmail());
            pstmt.setString(3, user.getCountry());
            int rowAffected = pstmt.executeUpdate();
            //Lấy user id
            rs = pstmt.getGeneratedKeys();
            int userId = 0;
            if (rs.next()){
                userId = rs.getInt(1);
            }
            //Trong TH thêm thành công, chỉ định quyền cho người dùng
            if(rowAffected == 1){
                String sqlPivot = INSERT_USER_PERMISSION_SQL;
                pstmtAssignment = connection.prepareStatement(sqlPivot);
                for (int permissionId: permisions) {
                    pstmtAssignment.setInt(1, userId);
                    pstmtAssignment.setInt(2, permissionId);
                    pstmtAssignment.executeUpdate();
                }
                connection.commit();
            }else {
                connection.rollback();
            }
        } catch (SQLException e) {
            try {
                if (connection != null)
                    connection.rollback();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
            System.out.println(e.getMessage());
        } finally {
          try {
              if (rs != null) rs.close();
              if (pstmt != null) pstmt.close();
              if (pstmtAssignment != null) pstmtAssignment.close();
              if (connection != null) connection.close();
          } catch (SQLException e) {
              System.out.println(e.getMessage());
          }
        }
    }
}
