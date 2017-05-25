package maze.app;

import maze.app.business.*;
import maze.data.DBConnector;
import org.postgresql.util.PSQLException;
import maze.data.PostgreSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Created by dvird on 17/03/29.
 */
public class Solution {

    private static boolean check_bad_param(SQLException e){
        int x = Integer.parseInt(e.getSQLState());
        if (x == PostgreSQLErrorCodes.INTEGRITY_CONSTRAINT_VIOLATION.getValue() ||
                x == PostgreSQLErrorCodes.RESTRICT_VIOLATION.getValue() ||
                x == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                x == PostgreSQLErrorCodes.CHECK_VIOLIATION.getValue()) {
            return true;
        }
        return false;
    }

    private static boolean check_already_exists(SQLException e){
        int x = Integer.parseInt(e.getSQLState());
        if (x == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
            return true;
        return false;
    }

    private static boolean check_foreign_dep(SQLException e){
        int x = Integer.parseInt(e.getSQLState());
        if (x == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue())
            return true;
        return false;
    }


    public static void createTables()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement create_users_query = null;
        PreparedStatement create_hops_query = null;
        try {
            create_hops_query = connection.prepareStatement("CREATE TABLE Hops\n" +
                    "(\n" +
                    "    source integer,\n" +
                    "    destination integer,\n" +
                    "    load integer NOT NULL,\n" +
                    "    PRIMARY KEY (source, destination),\n" +
                    "    CHECK (source <> destination),\n" +
                    "    CHECK (load > 0),\n" +
                    "    CHECK (source > 0),\n" +
                    "    CHECK (destination > 0)\n" +
                    ")");
            create_hops_query.execute();
            create_users_query = connection.prepareStatement("CREATE TABLE Users\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    source integer,\n" +
                    "    destination integer,\n" +
                    "    FOREIGN KEY (source, destination) REFERENCES Hops(source, destination),\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0),\n" +
                    "    CHECK (source <> destination)\n" +
                    ")");
            create_users_query.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                create_users_query.close();
                create_hops_query.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void clearTables()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("TRUNCATE Users,Hops");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Drops the tables from DB
     */
    public static void dropTables()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DROP TABLE Users,Hops");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  Adds a Hop to the database
     * @param hop
     * @return
     * OK in case of success,
     * BAD_PARAMS in case of illegal parametes,
     * ALREADY_EXISTS if hop already exists,
     * ERROR in case of database error
     */
    public static ReturnValue addHop(Hop hop)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement insert_hop_query = null;

        try {
            insert_hop_query = connection.prepareStatement("INSERT INTO Hops Values (" +
                    hop.getSource() + "," + hop.getDestination() + "," + hop.getLoad() + ")");
            insert_hop_query.execute();
        } catch (SQLException e){
            if (check_already_exists(e)){
                result = ReturnValue.ALREADY_EXISTS;
            } else if (check_bad_param(e)){
                result = ReturnValue.BAD_PARAMS;
            } else {
                result = ReturnValue.ERROR;
            }
        }
        finally {
            try {
                insert_hop_query.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     *  Returns a Hop according to given source and destination
     * @param source The source vertex
     * @param destination The destination vertex
     * @return
     * Hop with the wanted source and destination
     * BadHop in case the hop not found, or in case of server error
     */
    public static Hop getHop(int source, int destination)
    {
        Hop result = Hop.badHop;
        Connection connection = DBConnector.getConnection();
        PreparedStatement get_hop_query = null;

        try {
            get_hop_query = connection.prepareStatement("SELECT * FROM Hops where source = " + source +
                    " and destination = " + destination);
            ResultSet rs = get_hop_query.executeQuery();
            boolean once = false;
            while (rs.next()){
                once = true;
                int s = Integer.parseInt(rs.getString("source"));
                int d = Integer.parseInt(rs.getString("destination"));
                int l = Integer.parseInt(rs.getString("load"));
                result = new Hop(s, d, l);
            }
            rs.close();

        } catch (SQLException e){
            //it's already default as bad
        }
        finally {
            try {
                get_hop_query.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }


    /**
     *Updates a given existing Hop with new laod
     * @param hop A Hop contains updated load
     * @return
     * OK in case of success
     * NOT_EXISTS in case of the hop not existing
     * BAD_PARAMS in case of bad input load
     * ERROR in case of server error
     *
     */
    public static ReturnValue updateHopLoad(Hop hop)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement update_hop_query = null;

        try {
            if (getHop(hop.getSource(), hop.getDestination()).getSource() == -1){
                result = ReturnValue.NOT_EXISTS;
            } else {
                update_hop_query = connection.prepareStatement("UPDATE Hops set load = " + hop.getLoad() +
                        "WHERE source = " + hop.getSource() + " and destination = " + hop.getDestination());
                update_hop_query.execute();
            }
        } catch (SQLException e){
            if (check_bad_param(e)) {
                result = ReturnValue.BAD_PARAMS;
            } else {
                result = ReturnValue.ERROR;
            }
        }
        finally {
            try {
                if (update_hop_query != null) {
                    update_hop_query.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

	
	  /**
     * Deletes hop from the data base according to a given source and destination
     * @param source The source vertex
     * @param destination The destination vertex
     * @return
     * OK in case of success
     * NOT_EXISTS in case the hop does not exists
     * ERROR in case of other server error
     */
    public static ReturnValue deleteHop(int source, int destination)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement delete_hop_query = null;

        try {
            if (getHop(source, destination).getSource() == -1){
                result = ReturnValue.NOT_EXISTS;
            }
            else {
                delete_hop_query = connection.prepareStatement("DELETE FROM Hops WHERE source = " +
                        source + " and destination = " + destination);
                delete_hop_query.execute();
            }
        } catch (SQLException e){
            result = ReturnValue.ERROR;
        }
        finally {
            try {
                if (delete_hop_query != null) {
                    delete_hop_query.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Adds a user to the database
     * @param user
     * @return
     * OK in case of success,
     * BAD_PARAMS in case of illegal input parameters
     * ALREADY_EXISTS if user is allready exsists
     * NOT_EXISTS if the given user's Hop does not exists
     * ERROR in case of other server error
     */
    public static ReturnValue addUser(User user)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement insert_user_query = null;

        try {
            Hop user_hop = getHop(user.getSource(), user.getDestination());
            if (user.getSource() < 1 || user.getDestination() < 1 || user.getId() < 1){
                result = ReturnValue.BAD_PARAMS;
            } else if (user_hop.getSource() == -1){
                result = ReturnValue.NOT_EXISTS;
            } else {
                insert_user_query = connection.prepareStatement("INSERT INTO Users Values (" +
                        user.getId() + "," + user.getSource() + "," + user.getDestination() + ")");
                insert_user_query.execute();
            }

        } catch (SQLException e){
            if (check_already_exists(e)){
                result = ReturnValue.ALREADY_EXISTS;
            } else if (check_foreign_dep(e)){
                result = ReturnValue.NOT_EXISTS;
            } else if (check_bad_param(e)){
                result = ReturnValue.BAD_PARAMS;
            } else {
                result = ReturnValue.ERROR;
            }
        }
        finally {
            try {
                if (insert_user_query != null) {
                    insert_user_query.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Returns a User according to a given id
     * @param id input ID
     * @return
     * new User with the id of interest
     * badUser otherwise in the case of the user not found or server error
     */
    public static User getUser(int id)
    {
        User result = User.badUser;
        Connection connection = DBConnector.getConnection();
        PreparedStatement get_user_query = null;

        try {
            get_user_query = connection.prepareStatement("SELECT * FROM Users where id = " + id);
            ResultSet rs = get_user_query.executeQuery();
            boolean once = false;
            while (rs.next()){
                once = true;
                int s = Integer.parseInt(rs.getString("source"));
                int d = Integer.parseInt(rs.getString("destination"));
                result = new User(id, s, d);
            }
            rs.close();

        } catch (SQLException e){

        }
        finally {
            try {
                get_user_query.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;



    }

    /**
     * Update a user current hop
     * @param user a user with an updated hop
     * @return
     * OK in case of success
     * NOT_EXISTS if the user or the new hop does not exists
     * BAD_PARAMS in case of illegal parameters
     * ERROR in case of other server errors
     */
    public static ReturnValue updateUserHop(User user)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement update_user_query = null;

        try {
            if (getUser(user.getId()).getSource() == -1){
                result = ReturnValue.NOT_EXISTS;
            } else {
                update_user_query = connection.prepareStatement("UPDATE Users set source = " +
                        user.getSource() + ", destination = " + user.getDestination() + "WHERE id = " + user.getId());
                update_user_query.execute();
            }
        } catch (SQLException e){
            if (check_bad_param(e)) {
                result = ReturnValue.BAD_PARAMS;
            }
            else {
                result = ReturnValue.ERROR;
            }
        }
        finally {
            try {
                if (update_user_query != null) {
                    update_user_query.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Deletes user from the data base according to a given id
     * @param userId the id of the user
     * @return
     * OK in case of success
     * NOT_EXISTS in case the user does not exists
     * ERROR in case of other server error
     */
    public static ReturnValue deleteUser(int userId)
    {
        ReturnValue result = ReturnValue.OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement delete_user_query = null;

        try {
            if (getUser(userId).getSource() == -1){
                result = ReturnValue.NOT_EXISTS;
            }
            else {
                delete_user_query = connection.prepareStatement("DELETE FROM Users WHERE id = " + userId);
                delete_user_query.execute();
            }
        } catch (SQLException e){
            result = ReturnValue.ERROR;
        }
        finally {
            try {
                if (delete_user_query != null) {
                    delete_user_query.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     *
     * @param k - maximum results
     * @param usersThreshold - users threshold to filter hops with
     * @return
     * A list of the top-k actual loaded hops, where in each hop there are at least (greater or equal) usersThreshold users
     * hop's actual load is (hop's load * (current users in hop + 1))
     * The returned hops load field is their actual load
     *
     */
    public static ArrayList<Hop> topKLoadedHops(int k, int usersThreshold)
    {
        String query =
            "select hops.source, hops.destination, (count(*) + 1) * hops.load as actual_load from users/n" +
            "inner join hops on users.source = hops.source and users.destination = hops.destination/n" +
            "group by hops.source, hops.destination, hops.load/n" +
            "having count(*) > " + usersThreshold + "/n" +
            "order by actual_load desc/n" +
            "limit " + k;

       return null;
    }


    /**
     *
     * @param source source vertex
     * @param destination destination vertex
     * @param maxLength maximal length of the path in Hops
     * @return - A paths list containing all paths with length (in hops) which is less or equal to maxLength,
     * without cycles,
     * ordered by path's actual load
     */
    public static PathsList getAllPaths(int source, int destination, int maxLength)
    {
        return  null;
    }



}
