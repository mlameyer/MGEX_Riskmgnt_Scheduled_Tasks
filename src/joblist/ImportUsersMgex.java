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
class ImportUsersMgex {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportUsersMgex";
    private static DatabaseManager dbm;
    private static FTPManager ftp;
    private static String SFTPTOFILENAME;
    private static String filename;
    private static final String table = "t_data_globexusers";
    private static final String col_Names = "cme, globex, broker_id"
                        + ", mge, firm_id, mge2, clearing_member, mge3, "
                        + "clearing_member2, cust, n";
    private static final int id = 0;
    private static final boolean header = true;
    
    public ImportUsersMgex(Properties prop) 
    {
        this.prop = prop;
    }

    void grabUsersMgex() {
        
        String Timevalue = prop.getProperty("ImportUsersMgexExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        String SFTPHOST = prop.getProperty("SFTPHOST");
        String SFTPPORT = prop.getProperty("SFTPPORT");
        String SFTPUSER = prop.getProperty("SFTPUSER");
        String SFTPPASS = prop.getProperty("SFTPPASS");
        String SFTPWORKINGDIR = prop.getProperty("SFTPWORKINGDIR");
        String SFTPTOFILEPATH = prop.getProperty("SFTPTOFILEPATH");
        SFTPTOFILENAME = prop.getProperty("SFTPTOFILENAME");
        
        
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        ftp = new FTPManager(SFTPHOST,SFTPPORT,SFTPUSER,SFTPPASS,SFTPWORKINGDIR,SFTPTOFILEPATH);          
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportUsersMgex.RunnableJob(), 0, 86400, SECONDS);
        
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
                filename = SFTPTOFILENAME + ct.getCurrentDate();
                File filePath = ftp.getFile(filename);
                dbm.loadFlatFile(ProcedureName, filePath, table, col_Names, id, header);
            }
        }
    } 
    
}
