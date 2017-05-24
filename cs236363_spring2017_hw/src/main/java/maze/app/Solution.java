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

    public static void createTables()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt1 = null;
        PreparedStatement pstmt2 = null;
        PreparedStatement pstmt3 = null;
        try {

            pstmt1 = connection.prepareStatement("CREATE TABLE Places\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")");
            pstmt1.execute();
            pstmt2 = connection.prepareStatement("CREATE TABLE Users\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    source integer,\n" +
                    "    destination integer,\n" +
                    "    FOREIGN KEY (source) REFERENCES Places(id),\n" +
                    "    FOREIGN KEY (destination) REFERENCES Places(id),\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0),\n" +
                    "    CHECK (source <> destination)\n" +
                    ")");
            pstmt2.execute();
            pstmt3 = connection.prepareStatement("CREATE TABLE Hops\n" +
                    "(\n" +
                    "    source integer,\n" +
                    "    destination integer,\n" +
                    "    load integer NOT NULL,\n" +
                    "    FOREIGN KEY (source) REFERENCES Places(id)," +
                    "    FOREIGN KEY (destination) REFERENCES Places(id)," +
                    "    PRIMARY KEY (source, destination),\n" +
                    "    CHECK (source <> destination),\n" +
                    "    CHECK (load > 0)\n" +
                    ")");
            pstmt3.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt1.close();
                pstmt2.close();
                pstmt3.close();
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

            pstmt = connection.prepareStatement("TRUNCATE Hops,Users,Places");
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

            pstmt = connection.prepareStatement("DROP TABLE Hops,Users,Places");
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
        PreparedStatement add_source_to_places_query = null;
        PreparedStatement add_dest_to_places_query = null;
        PreparedStatement insert_hop_query = null;

        try {
            String source_str = Integer.toString(hop.getSource());
            String dest_str = Integer.toString(hop.getDestination());
            String load_str = Integer.toString(hop.getLoad());
            // Insert only if it's new
            add_source_to_places_query = connection.prepareStatement("INSERT INTO Places \nSELECT " + source_str +
                    " WHERE NOT EXISTS (SELECT id FROM Places WHERE id = " + source_str + ")"
                );
            add_source_to_places_query.execute();

            add_dest_to_places_query = connection.prepareStatement("INSERT INTO Places \nSELECT " + dest_str +
                    " WHERE NOT EXISTS (SELECT id FROM Places WHERE id = " + dest_str + ")"
            );
            add_dest_to_places_query.execute();

                insert_hop_query = connection.prepareStatement("INSERT INTO Hops Values (" +
                        source_str + "," + dest_str + "," + load_str + ")");
                insert_hop_query.execute();


        } catch (SQLException e){

            int x = Integer.parseInt(e.getSQLState());
            if (x == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                result = ReturnValue.ALREADY_EXISTS;
            } else if (x == PostgreSQLErrorCodes.INTEGRITY_CONSTRAINT_VIOLATION.getValue() ||
                    x == PostgreSQLErrorCodes.RESTRICT_VIOLATION.getValue() ||
                    x == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                    x == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue() ||
                    x == PostgreSQLErrorCodes.CHECK_VIOLIATION.getValue()){
                result = ReturnValue.BAD_PARAMS;
            } else {
                result = ReturnValue.ERROR;
            }
        }
        finally {
            try {
                add_source_to_places_query.close();
                add_dest_to_places_query.close();
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
        return null;
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
        return null;
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
       
        return null;
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
       return null;
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
        return null;
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

       return  null;
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
      return null;
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
