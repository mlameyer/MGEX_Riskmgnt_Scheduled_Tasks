/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mgex_riskmgnt_scheduled_tasks;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.swing.JOptionPane;
import joblist.Joblist;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class MGEX_Riskmgnt_Scheduled_Tasks {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Properties prop = new Properties();
        InputStream input = null;
    
    try {
            String filename = "config.properties";
            input = MGEX_Riskmgnt_Scheduled_Tasks.class.getClassLoader().getResourceAsStream(filename);
            if(input==null){
                System.out.println("Sorry, unable to find " + filename);
                JOptionPane.showMessageDialog(null,"Sorry, unable to find " + filename,"Warning",JOptionPane.WARNING_MESSAGE);
                System.exit(-1);
                return;
            }

            prop.load(input);
            Joblist jl = new Joblist(prop);
            jl.runJobs();

        } catch (IOException e) {
            System.out.println("Startup of application failed " + e);
        } finally
        {
            if(input!=null)
            {
                    try 
                    {
                        input.close();
                    } catch (IOException e) 
                    {
                    }
            }
        }
    }
}