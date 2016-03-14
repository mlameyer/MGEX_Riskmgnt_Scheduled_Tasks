/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package databasemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import joblist.CurrentTime;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class DatabaseManager {
    
    private final String JDBC_CONNECTION_URL;
    private final String DB_URL;
    private final String USER;
    private final String PASS;
    private final String JDBC_CONNECTION_URL1;
    private final String DB_URL1;
    private final String USER1;
    private final String PASS1;
    private final Properties prop;
            
    public DatabaseManager(String JDBC_CONNECTION_URL, String DB_URL, String USER, String PASS, String JDBC_CONNECTION_URL1, String DB_URL1, String USER1, String PASS1, Properties prop) 
    {
        this.JDBC_CONNECTION_URL = JDBC_CONNECTION_URL;
        this.DB_URL = DB_URL;
        this.USER = USER;
        this.PASS = PASS;
        this.JDBC_CONNECTION_URL1 = JDBC_CONNECTION_URL1;
        this.DB_URL1 = DB_URL1;
        this.USER1 = USER1;
        this.PASS1 = PASS1;
        this.prop = prop;
    }
    
    private Connection getConnection()
    {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL, USER,PASS);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }

    private Connection getConnection2() {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL1);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL1, USER1,PASS1);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }

    public void callStoredProcedure(String Storedproc, String ProcedureName) 
    {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        Connection dbConnection = getConnection();
	CallableStatement callableStatement = null;
        try {
            callableStatement = dbConnection.prepareCall(Storedproc);
            callableStatement.executeUpdate();
            successful = 1;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            successful = 0;
        } finally 
        {
 
            if (callableStatement != null) {
                try {
                    callableStatement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
        
    }
    
    public void loadCSV(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        
        PreparedStatement preparedStatement = null;
        Connection dbConnection = null;
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        
        String questionmarks = null;
        String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
        String TABLE_REGEX = "\\$\\{table\\}";
        String KEYS_REGEX = "\\$\\{keys\\}";
        String VALUES_REGEX = "\\$\\{values\\}";
            
        try {
            
        
            br = new BufferedReader(new FileReader(filePath));
            String[] headerRow;
                
            if (header == true) {
                String headerRowString = br.readLine();
                headerRow = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                headerRow = col_Names.split(",");
            }

            if (null == headerRow) {
                    throw new FileNotFoundException(
                                    "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
            }
            
            for(int i = 0; i < headerRow.length; i++)
            {
                if(i == 0)
                {
                    questionmarks = "?,";
                } else if(i < headerRow.length - 1)
                {
                    questionmarks = questionmarks + "?,";
                } else
                {
                    questionmarks = questionmarks + "?";
                }
                  
            }

            String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table);
            query = query.replaceFirst(KEYS_REGEX, col_Names);
            query = query.replaceFirst(VALUES_REGEX, questionmarks);
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            preparedStatement = dbConnection.prepareStatement(query);
            
            if(id == 1) 
            {
                dbConnection.createStatement().execute("DELETE FROM " + table);	
            }
            
            while((line = br.readLine()) != null)
            {

                String headerRowString = line;
                String[] rowCount = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                int index = 0;
                for(int i = 1; i <= rowCount.length; i++)
                {
                    String insert = rowCount[index];
                    
                    if(insert.contains("\""))
                    {
                       preparedStatement.setString(i, insert.subSequence(1, insert.lastIndexOf("\"")).toString());
                       index++;
                    }else
                    {
                       preparedStatement.setString(i, insert);
                       index++;
                    }
                    
                }
                
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } catch (IOException | SQLException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } finally 
        {
            try {
                if (br != null)
                    br.close();
                if (null != preparedStatement)
                    preparedStatement.close();
                if (null != dbConnection)
                    dbConnection.close();
            } catch (IOException | SQLException ex) {
                System.out.println("LoadCSV Failed " + ex);
                successful = 0;
            }
            
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    public void LoadFromAS400(String ProcedureName, String AS400query, String[] AS400Col_Names, String query) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        Connection dbConnection = getConnection();
        Connection dbConnection2 = getConnection2();
        
        try {
            ArrayList<String> list = new ArrayList<>();
            try (Statement stmt = dbConnection.createStatement()) {
                rs = stmt.executeQuery(AS400query);
                
                while(rs.next())
                {
                    for (String AS400Col_Name : AS400Col_Names) {
                        list.add(rs.getString(AS400Col_Name));
                    }
                }
            }
            
            
            int checkCount = 1;
            dbConnection2.setAutoCommit(false);
            preparedStatement = dbConnection2.prepareStatement(query);
            int i = 0, size = list.size();
            while(i < size){

                if(checkCount <= AS400Col_Names.length){
                    preparedStatement.setString(checkCount, (String) list.get(i));
                    i++;
                    checkCount++;
                } else{
                    preparedStatement.addBatch();
                    checkCount = 1;
                }
                if(i == size){
                    preparedStatement.addBatch();
                }
            }   
            preparedStatement.executeBatch();
            dbConnection2.commit();
            successful = 1;
            
            
        } catch (SQLException ex) {
            System.out.println("LoadFromAS400 Failed " + ex);
            successful = 0;
        } finally
        {
            try {
                if(rs != null)
                    rs.close();
                if(preparedStatement != null)
                    preparedStatement.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(dbConnection2 != null)
                    dbConnection2.close();
            } catch (SQLException ex) {
                System.out.println("LoadFromAS400 Failed " + ex);
                successful = 0;
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    private void updateStatusTable(String ProcedureName, String currentTime, int successful)
    {

        Connection dbConnection = getConnection2();
        Statement statement = null;
        String updateTable = "UPDATE mgex_riskmgnt_scheduled_tasks " + 
                "SET Last_Run_Time = '" + currentTime + "', " + 
                "Success = " + successful + " WHERE Procedures = '" + ProcedureName + "';";
        
        try 
        {
            statement = dbConnection.createStatement();

            statement.execute(updateTable);

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        } finally 
        {

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

        }
    }
    //Difference between truncateTable and truncateTable2 is getConnection()
    public void truncateTable(String table){
        try {
            try (Connection dbConnection = getConnection2()) {
                dbConnection.createStatement().execute("Truncate " + table);
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        
    }
    
    public void truncateTable2(String table){
        try {
            try (Connection dbConnection = getConnection()) {
                dbConnection.createStatement().execute("Truncate " + table);
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        
    }

    public void loadCSVstress(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatementUpdate = null;
        Connection dbConnection = null;
        Connection dbConnection2 = null;
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        
        String questionmarks = null;
        String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
        String TABLE_REGEX = "\\$\\{table\\}";
        String KEYS_REGEX = "\\$\\{keys\\}";
        String VALUES_REGEX = "\\$\\{values\\}";
        String SQL_Update = "UPDATE mgex_riskmgnt." + table + 
                        " SET Imported_File_Name = '" + filePath.getName() + "' Where " +
                        "Imported_File_Name IS NULL";
        
        try {
            
        
            br = new BufferedReader(new FileReader(filePath));
            String[] headerRow;
                
            if (header == true) {
                String headerRowString = br.readLine();
                headerRow = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                headerRow = col_Names.split(",");
            }

            if (null == headerRow) {
                    throw new FileNotFoundException(
                                    "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
            }
            
            for(int i = 0; i < headerRow.length; i++)
            {
                if(i == 0)
                {
                    questionmarks = "?,";
                } else if(i < headerRow.length - 1)
                {
                    questionmarks = questionmarks + "?,";
                } else
                {
                    questionmarks = questionmarks + "?";
                }
                  
            }

            String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table);
            query = query.replaceFirst(KEYS_REGEX, col_Names);
            query = query.replaceFirst(VALUES_REGEX, questionmarks);
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            preparedStatement = dbConnection.prepareStatement(query);
            
            if(id == 1) 
            {
                dbConnection.createStatement().execute("DELETE FROM " + table);	
            }
            
            while((line = br.readLine()) != null)
            {

                String headerRowString = line;
                String[] rowCount = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                int index = 0;
                for(int i = 1; i <= rowCount.length; i++)
                {
                    String insert = rowCount[index];
                    
                    if(insert.contains("\""))
                    {
                       preparedStatement.setString(i, insert.subSequence(1, insert.lastIndexOf("\"")).toString());
                       index++;
                    }else
                    {
                       preparedStatement.setString(i, insert);
                       index++;
                    }
                    
                }
                
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            dbConnection.commit();
            
            dbConnection2 = getConnection();
            preparedStatementUpdate = dbConnection2.prepareStatement(SQL_Update);
            preparedStatementUpdate.executeUpdate();

            successful = 1;
            
        } catch (FileNotFoundException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } catch (IOException | SQLException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } finally 
        {
            try {
                if (br != null)
                    br.close();
                if (null != preparedStatement)
                    preparedStatement.close();
                if (null != dbConnection)
                    dbConnection.close();
                if (null != preparedStatementUpdate)
                    preparedStatementUpdate.close();
                if (null != dbConnection2)
                    dbConnection2.close();
            } catch (IOException | SQLException ex) {
                System.out.println("LoadCSV Failed " + ex);
                successful = 0;
            }
            
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);

    }

    public void loadFlatFile(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        Connection dbConnection = null;
        PreparedStatement stmt = null;
        BufferedReader br = null;
        String query = "INSERT INTO t_data_globexusers (cme, globex, broker_id"
                        + ", mge, firm_id, mge2, clearing_member, mge3, clearing_member2, cust, n, FileDate)"
                        + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        
        try {
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            br = new BufferedReader(new FileReader(filePath));
            stmt = dbConnection.prepareStatement(query);
            String string, date;
            date = br.readLine();
            while(( string = br.readLine()) != null) 
            {
                stmt.setString(1, string.substring(0, 5));
                stmt.setString(2, string.substring(5, 9));
                stmt.setString(3, string.substring(9, 14));
                stmt.setString(4, string.substring(14, 19));
                stmt.setString(5, string.substring(19, 23));
                stmt.setString(6, string.substring(23, 27));
                stmt.setString(7, string.substring(30, 45));
                stmt.setString(8, string.substring(45, 50));
                stmt.setString(9, string.substring(50, 55));
                stmt.setString(10, string.substring(55, 60));
                stmt.setString(11, string.substring(60, 61));
                stmt.setString(12, date.substring(2, 9));
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException | SQLException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } catch (IOException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } finally
        {
            try {
                if(stmt != null)
                    stmt.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(br != null)
                    br.close();
            } catch (SQLException | IOException ex) {
                System.out.println("loadFlatFile Failed " + ex);
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    public void loadStandardFlatFile(String ProcedureName, String filePath, String table, String storedProcedure) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        Connection dbConnection = null;
        PreparedStatement stmt = null;
        BufferedReader br = null;
        String query = "INSERT INTO " + table + " (Col) VALUES (?)";
        
        truncateTable2(table);
        
        try {
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            br = new BufferedReader(new FileReader(filePath));
            stmt = dbConnection.prepareStatement(query);
            String string;

            while(( string = br.readLine()) != null) 
            {
                stmt.setString(1, string);
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException | SQLException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } catch (IOException ex) {
            System.out.println("loadFlatFile Failed " + ex);
            successful = 0;
        } finally
        {
            try {
                if(stmt != null)
                    stmt.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(br != null)
                    br.close();
            } catch (SQLException | IOException ex) {
                System.out.println("loadFlatFile Failed " + ex);
            }
        }
        
        callStoredProcedure(storedProcedure, ProcedureName);
        updateStatusTable(ProcedureName, currentTime, successful);
    }

    public void LoadFromAS400Trades(String ProcedureName, String AS400query, String AS400_SP, String query) {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        Connection dbConnection = getConnection();
        Connection dbConnection2 = getConnection2();
        
        try {
            dbConnection2.setAutoCommit(false);

            try (CallableStatement cStmt = dbConnection.prepareCall(AS400_SP)) {
                cStmt.execute();
                
                try (Statement stmt = dbConnection.createStatement()) {
                    rs = stmt.executeQuery(AS400query);
                    preparedStatement = dbConnection2.prepareStatement(query);

                    while (rs.next())
                    {
                        
                        preparedStatement.setString(1, rs.getString("TREXCH").trim());
                        preparedStatement.setString(2, rs.getString("TRFIRM").trim());
                        preparedStatement.setString(3, rs.getString("TRFRCH").trim());
                        preparedStatement.setString(4, rs.getString("TRDATE").trim());
                        preparedStatement.setInt(5, rs.getInt("TRTRID"));
                        preparedStatement.setString(6, rs.getString("TRCRDI").trim());
                        preparedStatement.setString(7, rs.getString("TRCRDS").trim());
                        preparedStatement.setString(8, rs.getString("TRCOMM").trim());
                        preparedStatement.setString(9, rs.getString("TRMON").trim());
                        preparedStatement.setString(10, rs.getString("TRYR2").trim());
                        preparedStatement.setInt(11, rs.getInt("TRSTRK"));
                        preparedStatement.setString(12, rs.getString("TRPBRO").trim());
                        preparedStatement.setString(13, rs.getString("TROBRO").trim());
                        preparedStatement.setString(14, rs.getString("TRPFIR").trim());
                        preparedStatement.setString(15, rs.getString("TROFIR").trim());
                        preparedStatement.setString(16, rs.getString("TRBS").trim());
                        preparedStatement.setInt(17, rs.getInt("TRQTY"));
                        preparedStatement.setInt(18, rs.getInt("TRTIME"));
                        preparedStatement.setInt(19, rs.getInt("TRPRIC"));
                        preparedStatement.setString(20, rs.getString("TRSTCD").trim());
                        preparedStatement.setInt(21, rs.getInt("TRCTI"));
                        preparedStatement.setString(22, rs.getString("TRACCT").trim());
                        preparedStatement.setString(23, rs.getString("TRORDN").trim());
                        preparedStatement.setString(24, rs.getString("TROCCD").trim());
                        preparedStatement.setString(25, rs.getString("TRSDAT").trim());
                        preparedStatement.setString(26, rs.getString("TRSTIM").trim());
                        preparedStatement.setString(27, rs.getString("TRMDAT").trim());
                        preparedStatement.setString(28, rs.getString("TRMTIM").trim());
                        preparedStatement.setString(29, rs.getString("TRMSTA").trim());
                        preparedStatement.setInt(30, rs.getInt("TRMTRI"));
                        preparedStatement.setInt(31, rs.getInt("TRMSEQ"));
                        preparedStatement.setString(32, rs.getString("TRCSTA").trim());
                        preparedStatement.setString(33, rs.getString("TRCOTS").trim());
                        preparedStatement.setString(34, rs.getString("TRLSTA").trim());
                        preparedStatement.setString(35, rs.getString("TRGUCD").trim());
                        preparedStatement.setString(36, rs.getString("TRGUSQ").trim());
                        preparedStatement.setString(37, rs.getString("TRGUOR").trim());
                        preparedStatement.setString(38, rs.getString("TRCABT").trim());
                        preparedStatement.setString(39, rs.getString("TRPCCD").trim());
                        preparedStatement.setString(40, rs.getString("TRELEC").trim());
                        preparedStatement.setString(41, rs.getString("TRCHNG").trim());
                        preparedStatement.setString(42, rs.getString("TRISRC").trim());
                        preparedStatement.setString(43, rs.getString("TRITID").trim());
                        preparedStatement.setString(44, rs.getString("TRIUSM").trim());
                        preparedStatement.setString(45, rs.getString("TRIENU").trim());
                        preparedStatement.setString(46, rs.getString("TRCDAT").trim());
                        preparedStatement.setString(47, rs.getString("TRSCOM").trim());
                        preparedStatement.setString(48, rs.getString("TRSTYP").trim());
                        preparedStatement.setString(49, rs.getString("TRSPRI").trim());
                        preparedStatement.setString(50, rs.getString("TROSTC").trim());

                        preparedStatement.addBatch();
                    }

                    preparedStatement.executeBatch();
                    dbConnection2.commit();
                    successful = 1;
                }
            }
        
        }catch(SQLException sqle){
            System.out.println("SQLException : " + sqle);
        }finally
        {
            try {
                if(rs != null)
                    rs.close();
                if(preparedStatement != null)
                    preparedStatement.close();
                if(dbConnection != null)
                    dbConnection.close();
                if(dbConnection2 != null)
                    dbConnection2.close();
            } catch (SQLException ex) {
                System.out.println("LoadFromAS400Trades Failed " + ex);
                successful = 0;
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
   
}
