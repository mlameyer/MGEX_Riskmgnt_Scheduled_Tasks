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
class ImportCGMPost {

    private final Properties prop;
    private static UpdatePostSPANData usd;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportCGMPost";
    private static DatabaseManager dbm;
    private static final String AS400query = "Select * from CLRDAT.CLRPOST";
    private static final String[] AS400Col_Names = {"POEXCH","POFIRM","POFRCH",
        "POCOMM","POMON","POYR4","POSTRK","PORBOT","PORSLD","POEBOT","POESLD",
        "POSBOT","POSSLD","POTBOT","POTSLD","POXBOT","POXSLD","PONPBL","PONPBS",
        "PONPEL","PONPES","POGPBL","POGPBS","POGPEL","POGPES","PODATE"};
    private static final String query = "INSERT INTO t_data_netclrpos(Exch,Firm,Type,Comm,Month,Year,Strike, "
          + "RegBot,RegSold,ExerBot,ExerSold,SPDBot,SPDSold,TFRBot,TFRSold, " 
          + "XCFOBot,XCFOSold,NetBegLong,NetBegShort,NetEndLong, "
          + "NetEndShort,GrossBegLong,GrossBegShort,GrossEndLong,GrossEndShort, " 
          + "PosDate) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String table = "t_data_netclrpos";
            
    public ImportCGMPost(Properties prop) 
    {
        this.prop = prop;
    }

    void csvImportCGMPost() {
        String Timevalue = prop.getProperty("importCGMPostExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL2");
        String DB_URL = prop.getProperty("remote_DB_URL2") + prop.getProperty("remote_DB_CLRDAT");
        String USER = prop.getProperty("remote_DB_USER2");
        String PASS = prop.getProperty("remote_DB_PASS2");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        usd = new UpdatePostSPANData(prop);
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportCGMPost.RunnableJob(), initalDelay, 86400, SECONDS);
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
                dbm.truncateTable(table);
                dbm.LoadFromAS400(ProcedureName, AS400query, AS400Col_Names, query);
                usd.csvImportCGMPost();
            } 
        }
    }
    
}
