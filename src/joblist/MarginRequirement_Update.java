/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import databasemanager.DatabaseManager;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class MarginRequirement_Update {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "MarginRequirement_Update";
    private final static String Storedproc = "CALL sp_Update_settlements()";
    private static DatabaseManager dbm;
            
    MarginRequirement_Update(Properties prop) 
    {
        this.prop = prop;
    }

    void getMargins() {
        
        String Timevalue = prop.getProperty("MarginRequirement_UpdateExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Mgn");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new MarginRequirement_Update.RunnableJob(), initalDelay, 86400, SECONDS);
        
    }

    private static class RunnableJob implements Runnable {
        
        @Override
        public void run() 
        {
            
            int result = ct.getDayofWeek();
            
            if(result == 5 || result == 6)
            {
                
            }
            else
            {
                dbm.callStoredProcedure(Storedproc, ProcedureName);
            }
        }
    }  
}
