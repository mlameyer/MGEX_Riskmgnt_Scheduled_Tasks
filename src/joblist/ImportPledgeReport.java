/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import databasemanager.DatabaseManager;
import ftpmanager.FTPManager;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ImportPledgeReport {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportPledgeReport";
    private static DatabaseManager dbm;
    private static FTPManager ftp;
    private static String SFTPPLEDGEREPORT;
    private static String filename;
    private static final String table = "t_data_pledge_report";
    private static final String col_Names = "AccountName, BroadridgeId, CUSIP,"
            + " PledgeAmount, MarketValue, Description, DateDate";
    private static final int id = 0;
    private static final boolean header = true;
    
    public ImportPledgeReport(Properties prop) {
        this.prop = prop;
    }

    void SFTPImport() {
        
        String Timevalue = prop.getProperty("ImportPledgeReportExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        String SFTPHOST1 = prop.getProperty("SFTPHOST1");
        String SFTPPORT1 = prop.getProperty("SFTPPORT1");
        String SFTPUSER1 = prop.getProperty("SFTPUSER1");
        String SFTPPASS1 = prop.getProperty("SFTPPASS1");
        String SFTPWORKINGDIR1 = prop.getProperty("SFTPWORKINGDIR1");
        SFTPPLEDGEREPORT = prop.getProperty("SFTPPLEDGEREPORT");
        String SFTPPLEDGEREPORTPath = prop.getProperty("SFTPPLEDGEREPORTPath");
        
        
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        ftp = new FTPManager(SFTPHOST1,SFTPPORT1,SFTPUSER1,SFTPPASS1,SFTPWORKINGDIR1,SFTPPLEDGEREPORTPath);          
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportPledgeReport.RunnableJob(), initalDelay, 86400, SECONDS);
        
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
                filename = SFTPPLEDGEREPORT.substring(0, SFTPPLEDGEREPORT.indexOf(".")) + ct.getPreviousDate() + SFTPPLEDGEREPORT.substring(SFTPPLEDGEREPORT.indexOf("."));
                File filePath = ftp.getFile(filename);
                dbm.loadCSV(ProcedureName, filePath, table, col_Names, id, header);
            }
        }
    } 
    
}
