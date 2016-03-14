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
class ImportIntraday {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportIntraday";
    private static DatabaseManager dbm;
    private static final String AS400query = "Select * from CLRDAT.CLRMGNT";
    private static final String[] AS400Col_Names = {"MGEXCH","MGFIRM","MGFRCH",
        "MGYBCS","MGYBNC","MGYMRQ","MGTFLF","MGTFLO","MGTFCF","MGTFCO","MGTTCS",
        "MGTTNC","MGTMRQ","MGTMOF","MGTCTI","MGTCTO","MGTBCS","MGTBNC","MGDATE",
        "MGTGAR"};
    private static final String query = "INSERT INTO t_data_intradaypaycollect (Exchange, Clearing_Member, "
                + "Reg_Seg, Yesterday_Cash_Balance, Yesterday_non_cash_Balance, "
                + "Yesterday_Margin_Requirement, Intraday_Variation, Option_Fluctuation, "
                + "UnKnown, NA, NA2, NA3, Current_Margin_Requirement, "
                + "NA4, End_of_Day_Pay, End_of_Day_Collect, New_Cash_Margin, "
                + "New_Noncash_Margin, Date2, Residual) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String table = "t_data_intradaypaycollect";
    
    public ImportIntraday(Properties prop) 
    {
        this.prop = prop;
    }

    void csvImportIntra() {
        String Timevalue = prop.getProperty("ImportIntradayExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL2");
        String DB_URL = prop.getProperty("remote_DB_URL2") + prop.getProperty("remote_DB_CLRDAT");
        String USER = prop.getProperty("remote_DB_USER2");
        String PASS = prop.getProperty("remote_DB_PASS2");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportIntraday.RunnableJob(), initalDelay, 86400, SECONDS);
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
            } 
        }
    }
    
}
