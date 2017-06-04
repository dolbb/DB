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
                int s = rs.getInt("source");
                int d = rs.getInt("destination");
                int l = rs.getInt("load");
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
     * ALREADY_EXISTS if user is already exists
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
                int s = rs.getInt("source");
                int d = rs.getInt("destination");
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
        Connection connection = DBConnector.getConnection();
        PreparedStatement top_k_loaded_hops_quarry = null;
        String queryString =
                "SELECT hops.source, hops.destination, (count(*) + 1) * hops.load AS actual_load FROM users\n" +
                        "INNER JOIN hops ON users.source = hops.source AND users.destination = hops.destination\n" +
                        "GROUP BY hops.source, hops.destination, hops.load\n" +
                        "HAVING count(*) >= " + usersThreshold + "\n" +
                        "ORDER BY actual_load DESC\n" +
                        "LIMIT " + k;
        ArrayList<Hop> result = new ArrayList<Hop>();
        try {
            top_k_loaded_hops_quarry = connection.prepareStatement(queryString);
            ResultSet rs = top_k_loaded_hops_quarry.executeQuery();
            while(rs.next()){
                result.add(new Hop(rs.getInt("source"), rs.getInt("destination"), rs.getInt("actual_load")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                top_k_loaded_hops_quarry.close();
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
     * @param source source vertex
     * @param destination destination vertex
     * @param maxLength maximal length of the path in Hops
     * @return - A paths list containing all paths with length (in hops) which is less or equal to maxLength,
     * without cycles,
     * ordered by path's actual load
     */
    public static PathsList getAllPaths(int source, int destination, int maxLength)
    {
        /* Example of our query - selecting all paths up to length 3 hops and returning by order of ascending actual load.
		
		---------- select all verticies and the sum of the actual load.
		SELECT col1, col2, col3, col4, COALESCE(L1.al, 0) + COALESCE(L2.al, 0) + COALESCE(L3.al, 0) AS len FROM 
		(
			----------  select all paths of 1 hops from source to destination (pad with 2 -1) ----------
			SELECT  -1 AS col1,  -1 AS col2, h1.source AS col3 , h1.destination AS col4 FROM hops h1
		UNION ALL
			----------  select all paths of 2 hops from source to destination (pad with 1 -1) ----------
			SELECT  -1, h2.source , h2.destination, h1.destination FROM hops h1
			INNER JOIN hops h2 ON h1.source = h2.destination
			WHERE 
				h2.source <> h1.destination
		UNION ALL
			----------  select all paths of 3 hops from source to destination ----------
			SELECT  h3.source , h3.destination, h2.destination, h1.destination FROM hops h1
			INNER JOIN hops h2 ON h1.source = h2.destination
			INNER JOIN hops h3 ON h2.source = h3.destination
			WHERE 
				h2.source <> h1.destination AND h3.source <> h1.destination AND h3.source <> h2.destination
		) AS A

		---------- get the actual load for the first hop (a hop with -1 will get 0 from the COALESCE func) ----------
		LEFT OUTER JOIN
		(
			SELECT hops.source AS s, hops.destination AS d, (count(*) + 1) * hops.load AS al FROM users 
			INNER JOIN hops ON users.source = hops.source AND users.destination = hops.destination 
			GROUP BY hops.source, hops.destination, hops.load
		) AS L1
		ON col1 = L1.s AND col2 = L1.d

		---------- get the actual load for the second hop ----------
		LEFT OUTER JOIN
		(
			SELECT hops.source AS s, hops.destination AS d, (count(*) + 1) * hops.load AS al FROM users 
			INNER JOIN hops ON users.source = hops.source AND users.destination = hops.destination 
			GROUP BY hops.source, hops.destination, hops.load
		) AS L2
		ON col2 = L2.s AND col3 = L2.d

		---------- get the actual load for the third hop ----------
		LEFT OUTER JOIN
		(
			SELECT hops.source AS s, hops.destination AS d, (count(*) + 1) * hops.load AS al FROM users 
			INNER JOIN hops ON users.source = hops.source AND users.destination = hops.destination 
			GROUP BY hops.source, hops.destination, hops.load
		) AS L3
		ON col3 = L3.s AND col4 = L3.d

		---------- order by their ascending sum of actual loads. ----------
		ORDER BY len ASC
		
		
		*/
		
		
		
		
        Connection connection = DBConnector.getConnection();
        PreparedStatement get_paths_query;
        PathsList pl = new PathsList();
		// The final query
        String query;
		// Only the query that returns the paths (no lengths)
        String paths_query = "";
		// join_part are all the joins we do for hops to get paths of size num_hops.
        String join_part = "";
		// select_part is an incremental variable that grows for every path size. 
		// It is responsible for selecting the right columns.
        String select_part = "";
		// circle_condition is an incremental variable that grows for every path size. 
		// It is responsible for making sure that we do not get paths with circles.
        String circle_condition = "";

		// Creating the sub query that get's all the paths without circles.
        for (int num_hops = 1; num_hops <= maxLength; num_hops++){
			// 1. Selecting the right columns
            select_part = ", h" + num_hops + ".destination" + select_part;
			// The incremental part might be similiar between queries of different sizes, but there are still differences.
            String current_select_part = "";
            if (num_hops > 1){
                current_select_part = "h" + num_hops +".source " + select_part;
            } else {
                current_select_part = "h1.source as col" + maxLength + ", h1.destination as col" + (maxLength + 1);
            }

            for (int i = 1; i < maxLength - num_hops + 1; i++){
                if (num_hops > 1) {
                    current_select_part = "-1, " + current_select_part;
                } else {
                    current_select_part = "-1 as col" + (maxLength - i) + ", " + current_select_part;
                }
            }
			// Jointing hops multiple times to get paths.
            if (num_hops > 1){
                //inner join hops h3 on h2.source = h3.destination
                join_part += "inner join hops h" + num_hops + " ON h" + (num_hops - 1) +
                        ".source = h" + num_hops + ".destination\n";
            }
			// Making sure that there are no circles in our paths.
            for (int i = 1; i < num_hops; i++){
                circle_condition += " AND h" + num_hops + ".source <> h" + i + ".destination";
            }
			// Union over all the num_hops sized paths. if num_hops < maxLength then pad with -1.
            String s_n_d = "h1.destination = " + destination + " AND h" + num_hops + ".source = " + source;
            String exact_num_hops_query = "SELECT " + current_select_part + " FROM hops h1\n" +
                    join_part + "WHERE \n" + s_n_d + circle_condition;
            if (num_hops > 1){
                paths_query += "\nUNION ALL\n" + exact_num_hops_query;
            } else {
                paths_query = exact_num_hops_query;
            }
        }
		// actual_load_join is the table with actual_load that we join for each hop in our path
        String actual_load_join = "LEFT OUTER JOIN \n" +
                "(SELECT hops.source AS s, hops.destination AS d, (count(*) + 1) * hops.load AS al FROM users \n" + "" +
                "INNER JOIN hops ON users.source = hops.source AND users.destination = hops.destination \n" +
                "GROUP BY hops.source, hops.destination, hops.load) AS L";
        select_part = "";
        join_part = "";
		// sum_part is an incremental variable responsible for the length of the path (sum of hops actual load)
        String sum_part = "";
        for (int num_hops = 1; num_hops <= maxLength + 1; num_hops++){
            select_part += "col" + num_hops + ", ";

            if (num_hops <= maxLength){
                String temp_sum = "COALESCE(L" + num_hops + ".al, 0)";
                sum_part += temp_sum;
                if (num_hops <= maxLength - 1){
                    sum_part += " + ";
                }
                join_part += actual_load_join + num_hops + "\n" +
                        "ON col" + num_hops + " = L" + num_hops +
                        ".s AND col" + (num_hops + 1) + " = L" + num_hops + ".d \n";
            }
        }
        query = "SELECT " + select_part + sum_part + " AS len FROM (\n" + paths_query +
                ") as A \n" + join_part + "\nORDER BY len ASC";
        try {
            get_paths_query = connection.prepareStatement(query);
            ResultSet rs = get_paths_query.executeQuery();
            while (rs.next()) {
                Path p = new Path();
                for (int i = 1; i <= maxLength; i++) {
                    int current_vertex = rs.getInt("col" + i);
                    if (current_vertex > -1){
                        int next_vertex = rs.getInt("col" + (i + 1));
                        p.addHop(new Hop(current_vertex, next_vertex));
                    }
                }
                pl.addPath(p);
            }
            rs.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return pl;
    }



}
