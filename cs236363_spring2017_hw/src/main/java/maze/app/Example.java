package maze.app;

import maze.data.DBConnector;

import maze.app.business.Hop;
import java.sql.*;


/**
 * Created by dvird on 17/03/29.
 */
public class Example {

    public static void main(String[] args) {

        Solution.dropTables();
        Solution.createTables();
        //Solution.clearTables();
        //Solution.dropTables();
        Hop a = new Hop(1,2,3);
        Hop b = new Hop(4,5,6);
        Solution.addHop(a);
        Solution.addHop(a);
        Solution.addHop(b);
        //System.out.println(Solution.getHop(a.getSource(), a.getDestination()).getSource());
        System.out.println(Solution.deleteHop(10, 20));
        System.out.println(Solution.deleteHop(1, 2));
        System.out.println(Solution.deleteHop(1, 2));

        System.out.println(Solution.updateHopLoad(new Hop(4,5,1)));
        System.out.println(Solution.updateHopLoad(new Hop(4,5,-1)));
        System.out.println(Solution.updateHopLoad(new Hop(4,7,1)));


        //        dropTable();
//        createTable();
//        insertIntoTable();
//        selectFromTable();
//        dropTable();


    }

    private static void dropTable()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS hello_world");
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

    private static void createTable()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE hello_world\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    short_text text ,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")");
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

    private static void insertIntoTable()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO hello_world" +
                    " VALUES (1, 'hello world!'), (2, 'goodbye world!')");
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

    private static void selectFromTable()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM hello_world");
            ResultSet results = pstmt.executeQuery();
            DBConnector.printResults(results);
            results.close();

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
}
