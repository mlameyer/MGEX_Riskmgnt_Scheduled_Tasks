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
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ImportTradeData {

    private final Properties prop;
    private static UpdateFCMExceptions ufcm;
    private static UpdateTradeData utd;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportTradeData";
    private static DatabaseManager dbm;
    private static final String AS400query = "Select * from CLMDAT.TEMPSPTRD";
    private static final String AS400_SP = "CALL CLMPGM.SPCNVTRP( ' ') ";
    private static final String[] AS400Col_Names = {"MGEXCH","MGFIRM","MGFRCH",
        "MGYBCS","MGYBNC","MGYMRQ","MGTFLF","MGTFLO","MGTFCF","MGTFCO","MGTTCS",
        "MGTTNC","MGTMRQ","MGTMOF","MGTCTI","MGTCTO","MGTBCS","MGTBNC","MGDATE",
        "MGTGAR"};
    private static final String query = "INSERT INTO t_datarlt_trades_wrk (" +
                    "TREXCH,TRFIRM,TRFRCH,TRDATE,TRTRID,TRCRDI,TRCRDS,TRCOMM," +
                    "TRMON,TRYR2,TRSTRK,TRPBRO,TROBRO,TRPFIR,TROFIR,TRBS," +
                    "TRQTY,TRTIME,TRPRIC,TRSTCD,TRCTI,TRACCT,TRORDN,TROCCD," +
                    "TRSDAT,TRSTIM,TRMDAT,TRMTIM,TRMSTA,TRMTRI,TRMSEQ,TRCSTA," +
                    "TRCOTS,TRLSTA,TRGUCD,TRGUSQ,TRGUOR,TRCABT,TRPCCD,TRELEC," +
                    "TRCHNG,TRISRC,TRITID,TRIUSM,TRIENU,TRCDAT,TRSCOM,TRSTYP," +
                    "TRSPRI,TROSTC) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
                    "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
                    "?,?,?,?,?,?)";
    private static final String table = "t_datarlt_trades_wrk";
            
    public ImportTradeData(Properties prop) 
    {
        this.prop = prop;
    }

    void TradeData() {
        String Timevalue = prop.getProperty("ImportTradeDataExecute");
        int min = Integer.parseInt(Timevalue.substring(Timevalue.indexOf(":") + 1, Timevalue.lastIndexOf(":")));
        String JDBC_CONNECTION_URL = prop.getProperty("remote_JDBC_CONNECTION_URL2");
        String DB_URL = prop.getProperty("remote_DB_URL2") + prop.getProperty("remote_DB_CLRDAT");
        String USER = prop.getProperty("remote_DB_USER2");
        String PASS = prop.getProperty("remote_DB_PASS2");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        ufcm = new UpdateFCMExceptions(prop);
        utd = new UpdateTradeData(prop);
        
        ct = new CurrentTime(prop);   

        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportTradeData.RunnableJob(), 0, min, MINUTES);
    }
    
    private static class RunnableJob implements Runnable {
        
        @Override
        public void run() 
        { 
            boolean open = ct.getMKOpen();
            
            if(open)
            {
                dbm.truncateTable(table);
                dbm.LoadFromAS400Trades(ProcedureName, AS400query, AS400_SP, query);
                utd.runProcedure();
                ufcm.runProcedure();
            }
        }
    }
    
}
