/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joblist;

import databasemanager.DatabaseManager;
import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class UpdatePostSPANData {
    
    private final Properties prop;
    private final static String ProcedureName = "ImportCGMPost";
    private final static String Storedproc = "CALL sp_update_post_spandata()";
    private static DatabaseManager dbm;
    
    public UpdatePostSPANData(Properties prop) 
    {
        this.prop = prop;
    }

    void csvImportCGMPost() {

        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        
        dbm.callStoredProcedure(Storedproc, ProcedureName);
        
    }  
}
