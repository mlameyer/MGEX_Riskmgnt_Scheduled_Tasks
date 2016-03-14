/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package joblist;

import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class Joblist {
    
    private final Properties prop;
    private int AllowJobtoExecute;
    
    public Joblist(Properties prop)
    {
        this.prop = prop;
    }

    public void runJobs() {

        ApplicationLock lock = new ApplicationLock();
        lock.bind();
        
        AllowJobtoExecute = Integer.parseInt(prop.getProperty("AllowJobtoExecute"));
        
        if(AllowJobtoExecute == 1)
        {
            ImportSettles tbl = new ImportSettles(prop);
            tbl.DataTransfer();
        }
        if(AllowJobtoExecute == 1)
        {
            ImportSpanPrice tbl2 = new ImportSpanPrice(prop);
            tbl2.DataTransfer();
            SpanPriceAlert spa = new SpanPriceAlert(prop);
            spa.DataTransfer();
        }

        ImportPledgeReport tbl3 = new ImportPledgeReport(prop);
        tbl3.SFTPImport();
/*      
        As of 8/24/2015 not used. manually done on CGM Website
        ImportMas90 tbl4 = new ImportMas90(prop);
        tbl4.csvImportMas90();
*/
        ImportPOEW tbl5 = new ImportPOEW(prop);
        tbl5.csvImportPOEW();
/*
        As of 8/24/2015 not used. manually done on CGM Website
        // Step six import paycollect.csv file
        ImportPaycollect tbl6 = new ImportPaycollect(prop);
        tbl6.csvImportPaycollect();
*/
        ImportCGMPost tbl9 = new ImportCGMPost(prop);
        tbl9.csvImportCGMPost();   
        
        ImportCGMPostAM am = new ImportCGMPostAM(prop);
        am.csvImportCGMPost();

        ImportIntraday tbl10 = new ImportIntraday(prop);
        tbl10.csvImportIntra();  
/*
        As of 8/24/2015 not used. manually done on CGM Website
        ImportStressTest tbl11 = new ImportStressTest(prop);
        tbl11.csvImport();
*/
        // Step twelve import USER_MGEX file from CME SFTP and import to DB
        ImportUsersMgex tbl12 = new ImportUsersMgex(prop);
        tbl12.grabUsersMgex();

        ImportTradeData tbl13 = new ImportTradeData(prop);
        tbl13.TradeData();

        MarginRequirement_Update tbl14 = new MarginRequirement_Update(prop);
        tbl14.getMargins();

        ImportDSROPaycollect tbl15 = new ImportDSROPaycollect(prop);
        tbl15.csvImportPaycollect();
        
        ImportOptionsOpenOutCry tbl16 = new ImportOptionsOpenOutCry(prop);
        tbl16.importFlatFile();

    }
}
