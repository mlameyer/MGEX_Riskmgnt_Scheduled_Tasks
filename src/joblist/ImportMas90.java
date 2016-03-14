/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import databasemanager.DatabaseManager;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ImportMas90 {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportMas90";
    private final static String Storedproc = "CALL sp_parseMAS90()";
    private static DatabaseManager dbm;
    private static String FullPathandName;
    private static String FilePath, FileName;
    private static final String table = "t_datafinance_mas90tb_wrk";
    private static final String col_Names = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,AA,AB,AC,AD,AE";
    private static final int id = 1;
    private static final boolean header = true;
    
    public ImportMas90(Properties prop) {
        this.prop = prop;
    }

    void csvImportMas90() {
        String Timevalue = prop.getProperty("ImportMas90Execute");
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        FilePath = prop.getProperty("Mas90FilePath");
        FileName = prop.getProperty("Mas90FileName");

        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
              
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportMas90.RunnableJob(), initalDelay, 86400, SECONDS);
    }
    
    private static class RunnableJob implements Runnable {
        
        @Override
        public void run() 
        {
            
            int result = ct.getDayofWeek();
            
            if(result == 0 || result == 6)
            {
                
            }
            else
            {
                FullPathandName = FilePath + FileName.substring(0, FileName.indexOf(".")) + ct.getPreviousDate() + FileName.substring(FileName.indexOf(".")); 
                File filePath = new File(FullPathandName);
                System.out.println(filePath);
                dbm.loadCSV(ProcedureName, filePath, table, col_Names, id, header);
                dbm.callStoredProcedure(Storedproc, ProcedureName);
                FullPathandName = null;
            }
        }
    }
    
}
