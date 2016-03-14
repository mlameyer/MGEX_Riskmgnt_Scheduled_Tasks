/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joblist;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import javax.swing.JOptionPane;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class ApplicationLock {

    File lockFile;

    ApplicationLock() {

    }
                

    void bind() {
        try {
            lockFile = new File("transport.lock");
            if (lockFile.exists())
                lockFile.delete();
            FileOutputStream lockFileOS = new FileOutputStream(lockFile);
            lockFileOS.close();
            FileChannel lockChannel = new RandomAccessFile(lockFile,"rw").getChannel();
            FileLock lock = lockChannel.tryLock();
            if (lock==null) throw new Exception("Unable to obtain lock");
        } catch (Exception e) 
        {
            System.out.println("Application MGEX_Riskmgnt_Scheduled_Tasks already running " + e);
            JOptionPane.showMessageDialog(null,"Application MGEX_Riskmgnt_Scheduled_Tasks already running","Warning",JOptionPane.WARNING_MESSAGE);
            System.exit(0);
        }
    }
    
}
