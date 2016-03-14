/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ftpmanager;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class FTPManager {

    private final String SFTPHOST;
    private final String SFTPPORT;
    private final String SFTPUSER;
    private final String SFTPPASS;
    private final String SFTPWORKINGDIR;
    private final String SFTPPLEDGEREPORTPath;
    Session     session     = null;
    Channel     channel     = null;
    ChannelSftp channelSftp = null;
            
    public FTPManager(String SFTPHOST1, String SFTPPORT1, String SFTPUSER1, String SFTPPASS1, String SFTPWORKINGDIR1, String SFTPPLEDGEREPORTPath) {
        this.SFTPHOST = SFTPHOST1;
        this.SFTPPORT = SFTPPORT1;
        this.SFTPUSER = SFTPUSER1;
        this.SFTPPASS = SFTPPASS1;
        this.SFTPWORKINGDIR = SFTPWORKINGDIR1;
        this.SFTPPLEDGEREPORTPath = SFTPPLEDGEREPORTPath;
    }
    
    public File getFile(String filename)
    {
        File newFile = null;
        
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTPUSER,SFTPHOST,Integer.parseInt(SFTPPORT));
            session.setPassword(SFTPPASS);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp)channel;
            channelSftp.cd(SFTPWORKINGDIR);
            byte[] buffer = new byte[1024];
            BufferedInputStream bis = new BufferedInputStream(channelSftp.get(filename));
            newFile = new File(SFTPPLEDGEREPORTPath + filename);
            OutputStream os = new FileOutputStream(newFile);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            int readCount;
            //System.out.println("Getting: " + theLine);
            
            while((readCount = bis.read(buffer)) > 0) {
                bos.write(buffer, 0, readCount);
            }
            
            bis.close();
            
            bos.close();

        } catch (JSchException | SftpException ex) {
            System.out.println(ex);
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
        
        return newFile;
    }
    
}
