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
class ImportOptionsOpenOutCry 
{
    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private static DatabaseManager dbm;
    private static final String importTable = "t_Flat_File_import_table";
    private static final String ProcedureName = "ImportOptionsOpenOutCry";
    private final static String storeProcedure = "CALL sp_parse_OO()";
    private static String file;
    
    public ImportOptionsOpenOutCry(Properties prop) 
    {
        this.prop = prop;
    }
    
    void importFlatFile() {
        String Timevalue = prop.getProperty("ImportOptionsOpenOutCryExecute");
        file = prop.getProperty("OptionsOpenOutCryFilePath");
        String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL1");
        String DB_URL = prop.getProperty("remote_DB_URL1") + prop.getProperty("remote_DB_Ops");
        String USER = prop.getProperty("remote_DB_USER1");
        String PASS = prop.getProperty("remote_DB_PASS1");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportOptionsOpenOutCry.RunnableJob(), initalDelay, 86400, SECONDS);
    }
    
    private static class RunnableJob implements Runnable 
    {
        
        @Override
        public void run() 
        {
            dbm.loadStandardFlatFile(ProcedureName, file, importTable, storeProcedure);
        }
    }
}
