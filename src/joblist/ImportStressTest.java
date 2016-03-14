/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import databasemanager.DatabaseManager;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.logging.Level;
import java.util.logging.Logger;
import resources.CleanDirectory;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ImportStressTest {

    private final Properties prop;
    private static DatabaseManager dbm;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportStressTest";
    private static final String cgmtbl = "t_data_stress_CGMresults";
    private static final String cmtbl = "t_data_stress_CMresults";
    private static final String lgttbl = "t_data_stress_lgtraderResults";
    private static final String col_Names = "Point_in_Time, Portfolio, Fut_Variation, Opt_Variation, Total_Variation, Percent_of_Ledger_Balance, Ledger_Balance";
    private static final int id = 0;
    private static final boolean header = true;
    private static String StressTestcgmcsv;
    private static String StressTestcmcsv;
    private static String StressTestlgtcsv;
    private static String StressTestFilePath;
    private static String StressTestBatchFilePath;
    private static String StressTestBatchFile;
            
    public ImportStressTest(Properties prop) 
    {
        this.prop = prop;
    }

    void csvImport() {
        String Timevalue = prop.getProperty("ImportStressTestExecute");
        String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER = prop.getProperty("local_DB_USER");
        String PASS = prop.getProperty("local_DB_PASS");
        String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
        String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
        String USER1 = prop.getProperty("local_DB_USER");
        String PASS1 = prop.getProperty("local_DB_PASS");
        
        StressTestcgmcsv = prop.getProperty("StressTestcgmcsv");
        StressTestcmcsv = prop.getProperty("StressTestcmcsv");
        StressTestlgtcsv = prop.getProperty("StressTestlgtcsv");
        StressTestFilePath = prop.getProperty("StressTestFilePath");
        StressTestBatchFilePath = prop.getProperty("StressTestBatchFilePath");
        StressTestBatchFile = prop.getProperty("StressTestBatchFile");
        
        ct = new CurrentTime(prop);   
        long initalDelay = ct.getTimeDelay(Timevalue);
        dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
        scheduler.scheduleAtFixedRate(new ImportStressTest.RunnableJob(), initalDelay, 86400, SECONDS);
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
                try {
                    Runtime.getRuntime().exec("cmd /c start " + StressTestBatchFile, null, new File(StressTestBatchFilePath));//cmd /c start 
                    Thread.sleep(120 * 1000);
                   
                    File dir = new File(StressTestFilePath);
                    List<String> list = Arrays.asList(dir.list(
                        new FilenameFilter() {
                            @Override public boolean accept(File dir, String name) {
                                return name.endsWith(".csv");
                            }
                        }
                    ));
                    
                    for(int i = 0; i < list.size(); i++)
                    {
                        String csvfile = list.get(i); 
                        if (csvfile.contains(StressTestcgmcsv))
                        {
                            
                            try 
                            {

                                dbm.loadCSVstress(ProcedureName, new File(StressTestFilePath + csvfile), cgmtbl, col_Names, id, header);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else if (csvfile.contains(StressTestcmcsv))
                                {

                            try {
                                
                                dbm.loadCSVstress(ProcedureName, new File(StressTestFilePath + csvfile), cmtbl, col_Names, id, header);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else if (csvfile.contains(StressTestlgtcsv)){
                            
                            try {

                                dbm.loadCSVstress(ProcedureName, new File(StressTestFilePath + csvfile), lgttbl, col_Names, id, header);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            
                        }     
                    }
                    
                    Path path = Paths.get(dir.toString());
                    System.out.println(path);
                    CleanDirectory.cleanDirectory(path);
                    
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(ImportStressTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            } 
        }
    }
}
