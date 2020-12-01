package flightapp;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;

/**
 * Runs queries against a back-end database
 */
public class Query {
  // DB Connection
  private Connection conn;

  // Added Fields
  private String username;
  private List<Itinerary> itineraries;

  // Password hashing parameter constants
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH = 128;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // For check dangling
  private static final String TRANCOUNT_SQL = "SELECT @@TRANCOUNT AS tran_count";
  private PreparedStatement tranCountStatement;

  // TODO: YOUR CODE HERE
  // For clearing tables
  private static final String CLEAR_USERS_SQL = "DELETE FROM Users";
  private PreparedStatement clearUsersStatement;

  private static final String CLEAR_RESERVATIONS_SQL = "DELETE FROM Reservations";
  private PreparedStatement clearReservationsStatement;

  // For creating new user account
  private static final String CREATE_USER_SQL = "INSERT INTO Users VALUES (?, ?, ?, ?)";
  private PreparedStatement createUserStatement;

  // For checking if user account exists in the table
  private static final String CHECK_USER_SQL = "SELECT COUNT(*) as count FROM Users WHERE username = ?";
  private PreparedStatement checkUserStatement;

  // For logging in user
  private static final String LOGIN_USER_SQL = "SELECT * FROM USERS WHERE username = ?";
  private PreparedStatement loginUserStatement;

  // For direct flight
  private static final String DIRECT_FLIGHT_SQL = "SELECT TOP (?) fid, day_of_month, carrier_id, flight_num, " +
          "origin_city, dest_city, actual_time, capacity, price " +
          "FROM Flights " +
          "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0 " +
          "ORDER BY actual_time ASC, fid ASC";
  private PreparedStatement directFlightStatement;

  // For indirect flight
  private static final String INDIRECT_FLIGHT_SQL = "SELECT TOP (?) + f1.fid AS f1_fid, " +
          "f1.day_of_month AS f1_day_of_month, " +
          "f1.carrier_id AS f1_carrier_id, " +
          "f1.flight_num AS f1_flight_num, " +
          "f1.origin_city AS f1_origin_city, " +
          "f1.dest_city AS f1_dest_city, " +
          "f1.actual_time AS f1_actual_time, " +
          "f1.capacity AS f1_capacity, " +
          "f1.price AS f1_price, " +

          "f2.fid AS f2_fid, " +
          "f2.day_of_month AS f2_day_of_month, " +
          "f2.carrier_id AS f2_carrier_id, " +
          "f2.flight_num AS f2_flight_num, " +
          "f2.origin_city AS f2_origin_city, " +
          "f2.dest_city AS f2_dest_city, " +
          "f2.actual_time AS f2_actual_time, " +
          "f2.capacity AS f2_capacity, " +
          "f2.price AS f2_price " +

          "FROM Flights AS f1, Flights AS f2 " +
          "WHERE f1.origin_city = ? AND f1.dest_city = f2.origin_city " +
          "AND f2.dest_city = ? AND f1.day_of_month = ? AND f2.day_of_month = ? " +
          "AND f1.canceled = 0 AND f2.canceled = 0 " +
          "ORDER BY f1.actual_time + f2.actual_time ASC, f1.fid ASC, f2.fid ASC";
  private PreparedStatement indirectFlightStatement;

  // For capacity of flight 1
  private static final String F1_CAPACITY_SQL = "SELECT COUNT(*) as count FROM Reservations " +
          "WHERE fid1 = ? AND canceled = 0";
  private PreparedStatement f1CapacityStatement;

  // For capacity of flight 1
  private static final String F2_CAPACITY_SQL = "SELECT COUNT(*) as count FROM Reservations " +
          "WHERE fid2 = ? AND canceled = 0";
  private PreparedStatement f2CapacityStatement;

  // For checking if the user has a reservation on the same day
  private static final String USER_SAME_DAY_BOOK_SQL = "SELECT COUNT(*) as count FROM Reservations " +
          "WHERE username = ? AND date = ? ";
  private PreparedStatement userSameDayBookStatement;

  // For getting the reservation ID
  private static final String GET_RESERVATION_ID_SQL = "SELECT COUNT(*) AS count from RESERVATIONS";
  private PreparedStatement getReservationIDStatement;

  // For adding reservation of the booking flight to the table
  private static final String BOOK_FLIGHT_SQL = "INSERT INTO Reservations VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement bookFlightStatement;

  // For getting the price from the reservation
  private static final String GET_RESERVATION_PRICE_SQL = "SELECT price FROM Reservations " +
          "WHERE username = ? AND id = ? AND paid = 0 AND canceled = 0";
  private PreparedStatement getReservationPriceStatement;

  // For getting the balance of the user
  private static final String GET_USER_BALANCE_SQL = "SELECT balance from Users where username = ?";
  private PreparedStatement getUserBalanceStatement;

  // For updating the paid status of the reservation
  private static final String UPDATE_PAID_STATUS_SQL = "UPDATE Reservations SET paid = 1 " +
          "WHERE username = ? AND id = ?";
  private PreparedStatement updatePaidStatusStatement;

  // For updating the balance of the user
  private static final String UPDATE_USER_BALANCE_SQL = "UPDATE Users SET balance = ? WHERE username = ?";
  private PreparedStatement updateUserBalanceStatement;

  // For getting the reservation of the user
  private static final String GET_USER_RESERVATION_SQL = "SELECT id, fid1, fid2, paid, date, price " +
          "FROM Reservations WHERE username = ? AND canceled = 0";
  private PreparedStatement getUserReservationStatement;

  // For getting the flight information with the fid
  private static final String GET_FLIGHT_INFO_SQL = "SELECT fid, day_of_month, carrier_id, flight_num," +
          "origin_city, dest_city, actual_time, capacity, price " +
          "FROM Flights WHERE fid = ? ";
  private PreparedStatement getFlightInfoStatement;

  // For getting the reservation to cancel
  private static final String GET_RESERVATION_CANCEL_SQL = "SELECT price, paid, canceled FROM Reservations " +
          "WHERE username = ? AND id = ?";
  private PreparedStatement getReservationCancelStatement;

  // For updating the cancel status of the reservation
  private static final String UPDATE_CANCEL_STATUS_SQL = "UPDATE Reservations SET canceled = 1 " +
          "WHERE username = ? AND id = ?";
  private PreparedStatement updateCancelStatusStatement;

  public Query() throws SQLException, IOException {
    this(null, null, null, null);
  }

  protected Query(String serverURL, String dbName, String adminName, String password)
      throws SQLException, IOException {
    conn = serverURL == null ? openConnectionFromDbConn()
        : openConnectionFromCredential(serverURL, dbName, adminName, password);

    prepareStatements();
  }

  /**
   * Return a connecion by using dbconn.properties file
   *
   * @throws SQLException
   * @throws IOException
   */
  public static Connection openConnectionFromDbConn() throws SQLException, IOException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("flightapp.server_url");
    String dbName = configProps.getProperty("flightapp.database_name");
    String adminName = configProps.getProperty("flightapp.username");
    String password = configProps.getProperty("flightapp.password");
    return openConnectionFromCredential(serverURL, dbName, adminName, password);
  }

  /**
   * Return a connecion by using the provided parameter.
   *
   * @param serverURL example: example.database.widows.net
   * @param dbName    database name
   * @param adminName username to login server
   * @param password  password to login server
   *
   * @throws SQLException
   */
  protected static Connection openConnectionFromCredential(String serverURL, String dbName,
      String adminName, String password) throws SQLException {
    String connectionUrl =
        String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
            dbName, adminName, password);
    Connection conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(true);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    return conn;
  }

  /**
   * Get underlying connection
   */
  public Connection getConnection() {
    return conn;
  }

  /**
   * Closes the application-to-database connection
   */
  public void closeConnection() throws SQLException {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      // TODO: YOUR CODE HERE
      clearUsersStatement.clearParameters();
      clearReservationsStatement.executeUpdate();
      clearUsersStatement.clearParameters();
      clearUsersStatement.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    tranCountStatement = conn.prepareStatement(TRANCOUNT_SQL);
    // TODO: YOUR CODE HERE
    clearUsersStatement = conn.prepareStatement(CLEAR_USERS_SQL);
    clearReservationsStatement = conn.prepareStatement(CLEAR_RESERVATIONS_SQL);
    createUserStatement = conn.prepareStatement(CREATE_USER_SQL);
    checkUserStatement = conn.prepareStatement(CHECK_USER_SQL);
    loginUserStatement = conn.prepareStatement(LOGIN_USER_SQL);
    directFlightStatement = conn.prepareStatement(DIRECT_FLIGHT_SQL);
    indirectFlightStatement = conn.prepareStatement(INDIRECT_FLIGHT_SQL);
    f1CapacityStatement = conn.prepareStatement(F1_CAPACITY_SQL);
    f2CapacityStatement = conn.prepareStatement(F2_CAPACITY_SQL);
    userSameDayBookStatement = conn.prepareStatement(USER_SAME_DAY_BOOK_SQL);
    getReservationIDStatement = conn.prepareStatement(GET_RESERVATION_ID_SQL);
    bookFlightStatement = conn.prepareStatement(BOOK_FLIGHT_SQL);
    getReservationPriceStatement = conn.prepareStatement(GET_RESERVATION_PRICE_SQL);
    getUserBalanceStatement = conn.prepareStatement(GET_USER_BALANCE_SQL);
    updatePaidStatusStatement = conn.prepareStatement(UPDATE_PAID_STATUS_SQL);
    updateUserBalanceStatement = conn.prepareStatement(UPDATE_USER_BALANCE_SQL);
    getUserReservationStatement = conn.prepareStatement(GET_USER_RESERVATION_SQL);
    getFlightInfoStatement = conn.prepareStatement(GET_FLIGHT_INFO_SQL);
    getReservationCancelStatement = conn.prepareStatement(GET_RESERVATION_CANCEL_SQL);
    updateCancelStatusStatement = conn.prepareStatement(UPDATE_CANCEL_STATUS_SQL);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged in\n" For all other
   *         errors, return "Login failed\n". Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    try {
      // TODO: YOUR CODE HERE
      // Check if user is already logged in
      if (this.username != null) {
        return "User already logged in\n";
      }
      try {
        // Set autocommit to false
        conn.setAutoCommit(false);
        // Get a table with the username parameter
        loginUserStatement.clearParameters();
        loginUserStatement.setString(1, username);
        ResultSet resultSet = loginUserStatement.executeQuery();

        // Check if user is registered
        if (!resultSet.next()) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Login failed\n";
        }

        // Get the hash and salt values for this username
        byte[] userHash = resultSet.getBytes("hash");
        byte[] userSalt = resultSet.getBytes("salt");

        // Generate a random cryptographic salt
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), userSalt, HASH_STRENGTH, KEY_LENGTH);
        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }

        // Check if two hashes are the same to log in
        if (Arrays.equals(userHash, hash)) {
          this.username = username;
          conn.commit();
          conn.setAutoCommit(true);
          return "Logged in as " + username + "\n";
        } else {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Login failed\n";
        }
      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)) {
            return transaction_login(username, password);
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        return "Login failed\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure
   *                   otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    try {
      // TODO: YOUR CODE HERE
      try {
        // Set autocommit to false
        conn.setAutoCommit(false);
        // Get a table with the username parameter
        checkUserStatement.clearParameters();
        checkUserStatement.setString(1, username);
        ResultSet resultSet = checkUserStatement.executeQuery();

        // Move the cursor to the next
        resultSet.next();
        // Get the value of the count
        int count = resultSet.getInt("count");
        resultSet.close();
        // Check if the username already exists
        if (count == 1) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Failed to create user\n";
        }
        // Check if the initial amount is negative
        if (initAmount < 0) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Failed to create user\n";
        }
      } catch (SQLException throwables) {
        throwables.printStackTrace();
      }

      // Generate a random cryptographic salt
      SecureRandom random = new SecureRandom();
      byte[] salt = new byte[16];
      random.nextBytes(salt);
      // Specify the hash parameters
      KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);
      // Generate the hash
      SecretKeyFactory factory = null;
      byte[] hash = null;
      try {
        factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        hash = factory.generateSecret(spec).getEncoded();
      } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
        throw new IllegalStateException();
      }

      // Set the values of the statement to insert the user into Users table
      createUserStatement.clearParameters();
      createUserStatement.setString(1, username);
      createUserStatement.setBytes(2, hash);
      createUserStatement.setBytes(3, salt);
      createUserStatement.setInt(4, initAmount);
      createUserStatement.executeUpdate();
      conn.commit();
      conn.setAutoCommit(true);
      return "Created user " + username + "\n";
    } catch (SQLException e) {
      try {
        conn.rollback();
        conn.setAutoCommit(true);
        // If the error is deadlock, then call this method recursively
        if (isDeadLock(e)) {
          return transaction_createCustomer(username, password, initAmount);
        }
      } catch (SQLException ex) {
        ex.printStackTrace();
      }
      return "Failed to create user\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination city, on the given day
   * of the month. If {@code directFlight} is true, it only searches for direct flights, otherwise
   * is searches for direct flights and flights with two "hops." Only searches for up to the number
   * of itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights, otherwise include
   *                            indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n". If an error
   *         occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total flight time]
   *         minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *         Itinerary numbers in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight,
      int dayOfMonth, int numberOfItineraries) {
    try {
      // WARNING the below code is unsafe and only handles searches for direct flights
      // You can use the below code as a starting reference point or you can get rid
      // of it all and replace it with your own implementation.
      //
      // TODO: YOUR CODE HERE
      StringBuffer sb = new StringBuffer();
      // List to store all the itineraries that matche to the user's selection
      itineraries = new ArrayList<>();

      try {
        // Set autocommit to false
        conn.setAutoCommit(false);
        // Set the values of the statement to get the flight info
        directFlightStatement.clearParameters();
        directFlightStatement.setInt(1, numberOfItineraries);
        directFlightStatement.setString(2, originCity);
        directFlightStatement.setString(3, destinationCity);
        directFlightStatement.setInt(4, dayOfMonth);
        // Get a table with the flight information
        ResultSet directResultSet = directFlightStatement.executeQuery();
        int count = 0;

        // Iterate the query result
        while (directResultSet.next()) {
          // Get all the information from the query result to create flights and itineraries
          int result_fid = directResultSet.getInt("fid");
          int result_dayOfMonth = directResultSet.getInt("day_of_month");
          String result_carrierId = directResultSet.getString("carrier_id");
          String result_flightNum = directResultSet.getString("flight_num");
          String result_originCity = directResultSet.getString("origin_city");
          String result_destCity = directResultSet.getString("dest_city");
          int result_time = directResultSet.getInt("actual_time");
          int result_capacity = directResultSet.getInt("capacity");
          int result_price = directResultSet.getInt("price");

          // Create a new Flight with the query result
          Flight flight = new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum,
                  result_originCity, result_destCity, result_time, result_capacity, result_price);
          // Add a new itinerary with the flight to itinerary list
          itineraries.add(new Itinerary(flight));
          count++;
        }
        directResultSet.close();
        // Check if we couldn't find any flights
        if (directFlight && count == 0) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "No flights match your selection\n";
        }

        if (!directFlight) {
          // Get (number of itineraries user input) - (number of itineraries counted)
          int indirectItineraries = numberOfItineraries - count;

          if (indirectItineraries > 0) {
            // Set the values of the statement to get the flight info
            indirectFlightStatement.clearParameters();
            indirectFlightStatement.setInt(1, indirectItineraries);
            indirectFlightStatement.setString(2, originCity);
            indirectFlightStatement.setString(3, destinationCity);
            indirectFlightStatement.setInt(4, dayOfMonth);
            indirectFlightStatement.setInt(5, dayOfMonth);
            // Get a table with the flight information
            ResultSet indirectResultSet = indirectFlightStatement.executeQuery();

            // Iterate the query result
            while (indirectResultSet.next()) {
              // Get all the information from the query result to create flights and itineraries
              // f1 indirect
              int f1_result_fid = indirectResultSet.getInt("f1_fid");
              int f1_result_dayOfMonth = indirectResultSet.getInt("f1_day_of_month");
              String f1_result_carrierId = indirectResultSet.getString("f1_carrier_id");
              String f1_result_flightNum = indirectResultSet.getString("f1_flight_num");
              String f1_result_originCity = indirectResultSet.getString("f1_origin_city");
              String f1_result_destCity = indirectResultSet.getString("f1_dest_city");
              int f1_result_time = indirectResultSet.getInt("f1_actual_time");
              int f1_result_capacity = indirectResultSet.getInt("f1_capacity");
              int f1_result_price = indirectResultSet.getInt("f1_price");
              // f2 indirect
              int f2_result_fid = indirectResultSet.getInt("f2_fid");
              int f2_result_dayOfMonth = indirectResultSet.getInt("f2_day_of_month");
              String f2_result_carrierId = indirectResultSet.getString("f2_carrier_id");
              String f2_result_flightNum = indirectResultSet.getString("f2_flight_num");
              String f2_result_originCity = indirectResultSet.getString("f2_origin_city");
              String f2_result_destCity = indirectResultSet.getString("f2_dest_city");
              int f2_result_time = indirectResultSet.getInt("f2_actual_time");
              int f2_result_capacity = indirectResultSet.getInt("f2_capacity");
              int f2_result_price = indirectResultSet.getInt("f2_price");
              // Create a new Flight f1 with the query result
              Flight f1 = new Flight(f1_result_fid, f1_result_dayOfMonth, f1_result_carrierId, f1_result_flightNum,
                      f1_result_originCity, f1_result_destCity, f1_result_time, f1_result_capacity, f1_result_price);
              // Create a new Flight f2 with the query result
              Flight f2 = new Flight(f2_result_fid, f2_result_dayOfMonth, f2_result_carrierId, f2_result_flightNum,
                      f2_result_originCity, f2_result_destCity, f2_result_time, f2_result_capacity, f2_result_price);
              // Add a new itinerary with two flights (f1, f2) to itinerary list
              itineraries.add(new Itinerary(f1, f2));
            }
            indirectResultSet.close();
          }
        }

        // Sort by total time and fid value (compareTo method)
        Collections.sort(itineraries);
        // Append the itineraries that we found to sb
        for (int i = 0; i < itineraries.size(); i++) {
          Itinerary itinerary = itineraries.get(i);
          sb.append("Itinerary " + i + ": " + itinerary.count + " flight(s), " + itinerary.totalTime + " minutes\n");
          sb.append(itinerary.toString());
        }
        conn.commit();
        conn.setAutoCommit(true);
        return sb.toString();

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)){
            return transaction_search(originCity, destinationCity, directFlight,
                    dayOfMonth, numberOfItineraries);
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return "Failed to search\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in
   *                    the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   *         If the user is trying to book an itinerary with an invalid ID or without having done a
   *         search, then return "No such itinerary {@code itineraryId}\n". If the user already has
   *         a reservation on the same day as the one that they are trying to book now, then return
   *         "You cannot book two flights in the same day\n". For all other errors, return "Booking
   *         failed\n".
   *
   *         And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n"
   *         where reservationId is a unique number in the reservation system that starts from 1 and
   *         increments by 1 each time a successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId) {
    try {
      // TODO: YOUR CODE HERE

      // Check if user is logged in
      if (username == null) {
        return "Cannot book reservations, not logged in\n";
      }

      // Check if the search result shows available itinerary
      // and the itineraryId is in the valid range
      if (itineraries == null || itineraryId < 0 || itineraryId >= itineraries.size()) {
        return "No such itinerary " + itineraryId + "\n";
      }
      try {
        // Set autocommit to false
        conn.setAutoCommit(false);

        // Get the itinerary with the itineraryId
        Itinerary bookItinerary = itineraries.get(itineraryId);

        // Check the capacity for the flight 1
        f1CapacityStatement.clearParameters();
        f1CapacityStatement.setInt(1, bookItinerary.f1.fid);
        ResultSet capacityResultSet = f1CapacityStatement.executeQuery();
        capacityResultSet.next();
        if (checkFlightCapacity(bookItinerary.f1.fid) - capacityResultSet.getInt("count") <= 0) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Booking failed\n";
        }
        capacityResultSet.close();

        // Check the capacity for the flight 2
        if (bookItinerary.f2 != null) {
          f2CapacityStatement.clearParameters();
          f2CapacityStatement.setInt(1, bookItinerary.f2.fid);
          capacityResultSet = f2CapacityStatement.executeQuery();
          capacityResultSet.next();
          if (checkFlightCapacity(bookItinerary.f2.fid) - capacityResultSet.getInt("count") <= 0) {
            conn.rollback();
            conn.setAutoCommit(true);
            return "Booking failed\n";
          }
          capacityResultSet.close();
        }

        // Check if user already has a reservation on the same day
        userSameDayBookStatement.clearParameters();
        userSameDayBookStatement.setString(1, username);
        userSameDayBookStatement.setInt(2, bookItinerary.f1.dayOfMonth);
        ResultSet sameDayResultSet = userSameDayBookStatement.executeQuery();
        sameDayResultSet.next();
        if (sameDayResultSet.getInt("count") > 0) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "You cannot book two flights in the same day\n";
        }
        sameDayResultSet.close();

        // Get a reservation id for booking
        getReservationIDStatement.clearParameters();
        ResultSet nextIDResultSet = getReservationIDStatement.executeQuery();
        nextIDResultSet.next();
        int reservationID = nextIDResultSet.getInt("count") + 1;
        nextIDResultSet.close();

        // Fill out the information of the inserting statement for the reservation
        int price = bookItinerary.f1.price;
        bookFlightStatement.clearParameters();
        bookFlightStatement.setInt(1, reservationID);
        bookFlightStatement.setString(2, username);
        bookFlightStatement.setInt(3,bookItinerary.f1.fid);
        if (bookItinerary.f2 != null) {
          bookFlightStatement.setInt(4,bookItinerary.f2.fid);
          price += bookItinerary.f2.price;
        } else {
          bookFlightStatement.setNull(4,java.sql.Types.INTEGER);
        }
        bookFlightStatement.setInt(5,0);
        bookFlightStatement.setInt(6, 0);
        bookFlightStatement.setInt(7,bookItinerary.f1.dayOfMonth);
        bookFlightStatement.setInt(8,price);
        bookFlightStatement.executeUpdate();
        conn.commit();
        conn.setAutoCommit(true);
        return "Booked flight(s), reservation ID: " + reservationID + "\n";

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)){
            return transaction_book(itineraryId);
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return "Booking failed\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n" If the reservation
   *         is not found / not under the logged in user's name, then return "Cannot find unpaid
   *         reservation [reservationId] under user: [username]\n" If the user does not have enough
   *         money in their account, then return "User has only [balance] in account but itinerary
   *         costs [cost]\n" For all other errors, return "Failed to pay for reservation
   *         [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining balance:
   *         [balance]\n" where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay(int reservationId) {
    try {
      // TODO: YOUR CODE HERE

      // Check if user is logged in
      if (username == null) {
        return "Cannot pay, not logged in\n";
      }
      try {
        // Set autocommit to false
        conn.setAutoCommit(false);

        // Get the price of the unpaid reservation
        getReservationPriceStatement.clearParameters();
        getReservationPriceStatement.setString(1, username);
        getReservationPriceStatement.setInt(2, reservationId);
        ResultSet priceResultSet = getReservationPriceStatement.executeQuery();

        // Check if the reservation exists
        if (!priceResultSet.next()) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Cannot find unpaid reservation " + reservationId + " under user: " + username + "\n";
        }

        // Get the price and paid status of the reservation
        int price = priceResultSet.getInt("price");
        priceResultSet.close();

        // Get the balance of the user
        getUserBalanceStatement.clearParameters();
        getUserBalanceStatement.setString(1, username);
        ResultSet balanceResultSet = getUserBalanceStatement.executeQuery();
        int balance = 0;
        if (balanceResultSet.next()) {
          balance = balanceResultSet.getInt("balance");
        }
        balanceResultSet.close();

        // Check if the price is greater than the user balance
        if (price > balance) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "User has only " + balance + " in account but itinerary costs " + price + "\n";
        }

        // Update the paid status of the reservation
        updatePaidStatusStatement.clearParameters();
        updatePaidStatusStatement.setString(1, username);
        updatePaidStatusStatement.setInt(2, reservationId);
        updatePaidStatusStatement.executeUpdate();

        // Update the balance of the user
        updateUserBalanceStatement.clearParameters();
        updateUserBalanceStatement.setInt(1, balance - price);
        updateUserBalanceStatement.setString(2, username);
        updateUserBalanceStatement.executeUpdate();

        conn.commit();
        return "Paid reservation: " + reservationId + " remaining balance: " + (balance - price) + "\n";

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)){
            return transaction_pay(reservationId);
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return "Failed to pay for reservation " + reservationId + "\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n" If
   *         the user has no reservations, then return "No reservations found\n" For all other
   *         errors, return "Failed to retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n [flight 1 under the
   *         reservation]\n [flight 2 under the reservation]\n Reservation [reservation ID] paid:
   *         [true or false]:\n [flight 1 under the reservation]\n [flight 2 under the
   *         reservation]\n ...
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    try {
      // TODO: YOUR CODE HERE

      // Check if user is logged in
      if (username == null) {
        return "Cannot view reservations, not logged in\n";
      }

      try {
        // String buffer to build a reservation information string
        StringBuffer sb = new StringBuffer();

        // Set autocommit to false
        conn.setAutoCommit(false);

        // Get the reservation with the username
        getUserReservationStatement.clearParameters();
        getUserReservationStatement.setString(1, username);
        ResultSet reservationResultSet = getUserReservationStatement.executeQuery();

        // Iterate the query result
        while (reservationResultSet.next()) {
          // Get the information of the result from the executed query
          int id = reservationResultSet.getInt("id");
          int fid1 = reservationResultSet.getInt("fid1");
          int fid2 = reservationResultSet.getInt("fid2");
          int paid = reservationResultSet.getInt("paid");
          int price = reservationResultSet.getInt("price");
          boolean isPaid = false;

          // Check if the reservation is already paid
          if (paid == 1) {
            isPaid = true;
          }

          // Get the information of the flight 1
          getFlightInfoStatement.clearParameters();
          getFlightInfoStatement.setInt(1,fid1);
          ResultSet f1ResultSet = getFlightInfoStatement.executeQuery();
          f1ResultSet.next();
          int f1_result_fid = f1ResultSet.getInt("fid");
          int f1_result_dayOfMonth = f1ResultSet.getInt("day_of_month");
          String f1_result_carrierId = f1ResultSet.getString("carrier_ID");
          String f1_result_flightNum = f1ResultSet.getString("flight_num");
          String f1_result_originCity = f1ResultSet.getString("origin_city");
          String f1_result_destCity = f1ResultSet.getString("dest_city");
          int f1_result_time = f1ResultSet.getInt("actual_time");
          int f1_result_capacity = f1ResultSet.getInt("capacity");
          int f1_result_price = f1ResultSet.getInt("price");

          // Create a new Flight f1 with the query result
          Flight f1 = new Flight(f1_result_fid, f1_result_dayOfMonth, f1_result_carrierId, f1_result_flightNum,
                  f1_result_originCity, f1_result_destCity, f1_result_time, f1_result_capacity, f1_result_price);

          // Check if flight 2 also exists in this reservation
          if (fid2 == 0) {
            // Append f1 with its information
            sb.append("Reservation " + id + " paid: " + isPaid + ":\n" +  f1.toString() + "\n");
          } else {
            // Get the information of flight 2
            getFlightInfoStatement.clearParameters();
            getFlightInfoStatement.setInt(1, fid2);
            ResultSet f2ResultSet = getFlightInfoStatement.executeQuery();
            f2ResultSet.next();
            int f2_result_fid = f2ResultSet.getInt("fid");
            int f2_result_dayOfMonth = f2ResultSet.getInt("day_of_month");
            String f2_result_carrierId = f2ResultSet.getString("carrier_ID");
            String f2_result_flightNum = f2ResultSet.getString("flight_num");
            String f2_result_originCity = f2ResultSet.getString("origin_city");
            String f2_result_destCity = f2ResultSet.getString("dest_city");
            int f2_result_time = f2ResultSet.getInt("actual_time");
            int f2_result_capacity = f2ResultSet.getInt("capacity");
            int f2_result_price = f2ResultSet.getInt("price");

            // Create a new Flgiht f2 with the query result
            Flight f2 = new Flight(f2_result_fid, f2_result_dayOfMonth, f2_result_carrierId, f2_result_flightNum,
                    f2_result_originCity, f2_result_destCity, f2_result_time, f2_result_capacity, f2_result_price);

            // Append f1 and f2 with their information
            sb.append("Reservation " + id +  " paid: " + isPaid +":\n" +  f1.toString() + "\n" +
                    f2.toString() +"\n");
          }
        }

        // Check if the reservation does not exist
        if (sb.length() == 0) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "No reservations found\n";
        }
        conn.commit();
        conn.setAutoCommit(true);
        // Return the StringBuffer that we built to string
        return sb.toString();

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)){
            return transaction_reservations();
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return "Failed to retrieve reservations\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n" For
   *         all other errors, return "Failed to cancel reservation [reservationId]\n"
   *
   *         If successful, return "Canceled reservation [reservationId]\n"
   *
   *         Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId) {
    try {
      // TODO: YOUR CODE HERE

      // Check if the user already logged in
      if (username == null) {
        return "Cannot cancel reservations, not logged in\n";
      }
      try {
        // Set autocommit to false
        conn.setAutoCommit(false);

        // Get the information of the reservation with the reservationId parameter
        getReservationCancelStatement.clearParameters();
        getReservationCancelStatement.setString(1, username);
        getReservationCancelStatement.setInt(2, reservationId);
        ResultSet cancelResultSet = getReservationCancelStatement.executeQuery();
        if (!cancelResultSet.next()) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Failed to cancel reservation " + reservationId + "\n";
        }

        // Check if the reservation is already canceled
        if (cancelResultSet.getInt("canceled") == 1) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Failed to cancel reservation " + reservationId + "\n";
        }

        // Get the paid status and the price of the reservation
        int paid = cancelResultSet.getInt("paid");
        int price = cancelResultSet.getInt("price");

        // Update the canceled status of the reservation.
        updateCancelStatusStatement.clearParameters();
        updateCancelStatusStatement.setString(1, username);
        updateCancelStatusStatement.setInt(2, reservationId);
        updateCancelStatusStatement.executeUpdate();

        // Check if the reservation is paid
        if (paid == 1) {
          // Get the balance of the user
          getUserBalanceStatement.clearParameters();
          getUserBalanceStatement.setString(1, username);
          ResultSet balanceResultSet = getUserBalanceStatement.executeQuery();
          balanceResultSet.next();

          // Update the balance of the user by adding the refund price
          int userBalance = balanceResultSet.getInt("balance");
          updateUserBalanceStatement.clearParameters();
          updateUserBalanceStatement.setInt(1,userBalance + price);
          updateUserBalanceStatement.setString(2, username);
          updateUserBalanceStatement.executeUpdate();
        }
        conn.commit();
        conn.setAutoCommit(true);
        return "Canceled reservation " + reservationId + "\n";

      } catch (SQLException e) {
        try {
          conn.rollback();
          conn.setAutoCommit(true);
          // If the error is deadlock, then call this method recursively
          if (isDeadLock(e)) {
            return transaction_cancel(reservationId);
          }
        } catch (SQLException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return "Failed to cancel reservation " + reservationId + "\n";
      }
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  /**
   * Throw IllegalStateException if transaction not completely complete, rollback.
   * 
   */
  private void checkDanglingTransaction() {
    try {
      try (ResultSet rs = tranCountStatement.executeQuery()) {
        rs.next();
        int count = rs.getInt("tran_count");
        if (count > 0) {
          throw new IllegalStateException(
              "Transaction not fully commit/rollback. Number of transaction in process: " + count);
        }
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Database error", e);
    }
  }

  private static boolean isDeadLock(SQLException ex) {
    return ex.getErrorCode() == 1205;
  }

  /**
   * A class to store flight information.
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public Flight(int fid, int dayOfMonth, String carrierId, String flightNum,
                  String originCity, String destCity, int time, int capacity, int price) {
      this.fid = fid;
      this.dayOfMonth = dayOfMonth;
      this.carrierId = carrierId;
      this.flightNum = flightNum;
      this.originCity = originCity;
      this.destCity = destCity;
      this.time = time;
      this.capacity = capacity;
      this.price = price;
    }

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
          + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
          + " Capacity: " + capacity + " Price: " + price;
    }
  }

  class Itinerary implements Comparable<Itinerary> {
    public Flight f1;
    public Flight f2;
    public int totalTime;
    public int count;

    // Direct flight
    public Itinerary(Flight f1) {
      this.f1 = f1;
      this.totalTime = f1.time;
      this.count = 1;
    }

    // Indirect flight
    public Itinerary(Flight f1, Flight f2) {
      this.f1 = f1;
      this.f2 = f2;
      this.totalTime = f1.time + f2.time;
      this.count = 2;
    }

    @Override
    public String toString() {
      if (count == 1) {
        return this.f1.toString() + "\n";
      } else {
        return this.f1.toString() + "\n" + this.f2.toString() + "\n";
      }
    }

    // Compare total time and fid value of itineraries
    @Override
    public int compareTo(Itinerary o) {
      int time = this.totalTime - o.totalTime;
      if (time != 0) {
        return time;
      } else {
        int fid1 = this.f1.fid - o.f1.fid;
        if (fid1 == 0 && this.f2 != null && o.f2 != null) {
          return this.f2.fid - o.f2.fid;
        }
        return fid1;
      }
    }
  }
}
