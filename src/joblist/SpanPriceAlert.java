/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joblist;

import databasemanager.DatabaseManager;
import email.SendEmailAttachment;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.MINUTES;
import resources.CleanDirectory;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class SpanPriceAlert {
    
    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "SpanPriceAlert";
    private final static String Storedproc = "CALL sp_rlt_price_alert()";
    private static DatabaseManager dbm;
    private static SendEmailAttachment email;
    private static String RealTimePriceAlertFilePath;
    
    SpanPriceAlert(Properties prop) {
        this.prop = prop;
    }
    
    void DataTransfer() {
        
        String value = prop.getProperty("SpanPriceAlert");
        int min = Integer.parseInt(value.substring(value.indexOf(":") + 1, value.lastIndexOf(":")));
        
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        String email_host = prop.getProperty("email_host");
        String email_port = prop.getProperty("email_port");
        RealTimePriceAlertFilePath = prop.getProperty("RealTimePriceAlertFilePath");
        String RealTimePriceAlertFileName = prop.getProperty("RealTimePriceAlertFileName");
        String RealTimePriceAlertMessage = prop.getProperty("RealTimePriceAlertMessage");
        String RealTimePriceAlertSubject = prop.getProperty("RealTimePriceAlertSubject");
        String RealTimePriceAlertFROM = prop.getProperty("RealTimePriceAlertFROM");
        String RealTimePriceAlertTO = prop.getProperty("RealTimePriceAlertTO");

        String delims = "[,]";
        String[] emails = RealTimePriceAlertTO.split(delims);
        
        ct = new CurrentTime(prop);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        email = new SendEmailAttachment(emails, RealTimePriceAlertFROM, email_host, email_port, RealTimePriceAlertSubject, RealTimePriceAlertMessage, RealTimePriceAlertFilePath, RealTimePriceAlertFileName);
        scheduler.scheduleAtFixedRate(new SpanPriceAlert.RunnableJob(), 1, min, MINUTES);
        
    }
    
    private static class RunnableJob implements Runnable {
        
        @Override
        public void run() 
        {
     
            boolean open = ct.getMKOpen();
            
            if(open)
            {
                dbm.callStoredProcedure(Storedproc, ProcedureName);
                File f = new File(RealTimePriceAlertFilePath); 
                if(f.exists())
                {
                    email.send();
                    Path path = Paths.get(RealTimePriceAlertFilePath);
                    System.out.println(path);
                    CleanDirectory.cleanDirectory(path);
                }else 
                {
                    System.out.println("File not found!");
                }
            }
        }
    }
    
}
