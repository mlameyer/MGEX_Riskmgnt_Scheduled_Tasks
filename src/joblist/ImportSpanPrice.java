/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import databasemanager.DatabaseManager;
import email.SendEmailAttachment;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ImportSpanPrice {

    private final Properties prop;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportSpanPrice";
    private final static String ProcedureName2 = "SpanPriceAlert";
    private final static String Storedproc = "CALL sp_export_span_price()";
    private final static String Storedproc2 = "CALL sp_rlt_price_alert()";
    private static DatabaseManager dbm;
    private static SendEmailAttachment email;
    private static String RealTimePriceAlertFilePath;
            
    ImportSpanPrice(Properties prop) {
        this.prop = prop;
    }

    void DataTransfer() {
        
        String value = prop.getProperty("ImportSpanPrice");
        int min = Integer.parseInt(value.substring(value.indexOf(":") + 1, value.lastIndexOf(":")));
        
        String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL1");
        String DB_URL = prop.getProperty("remote_DB_URL1") + prop.getProperty("remote_DB_Ops");
        String USER = prop.getProperty("remote_DB_USER1");
        String PASS = prop.getProperty("remote_DB_PASS1");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        ct = new CurrentTime(prop);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new RunnableJob(), 0, min, MINUTES);
        
    }

    private static class RunnableJob implements Runnable {
        
        @Override
        public void run() 
        {
     
            boolean open = ct.getMKOpen();
            
            if(open)
            {
                dbm.callStoredProcedure(Storedproc, ProcedureName);
            }
        }
    }  
}
