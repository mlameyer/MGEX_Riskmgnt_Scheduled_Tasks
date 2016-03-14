/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package email;

import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class SendEmailAttachment {

    private String[] emails = null;
    private String FROM = null;
    private String email_host = null;
    private String email_port = null;
    private String Subject = null;
    private String MessageBody = null;
    private String FilePath = null;
    private String FileName = null;
    
    public SendEmailAttachment(String[] emails, String RealTimePriceAlertFROM, String email_host, String email_port, String RealTimePriceAlertSubject, String RealTimePriceAlertMessage, String RealTimePriceAlertFilePath, String RealTimePriceAlertFileName) {
        this.emails = emails;
        this.FROM = RealTimePriceAlertFROM;
        this.email_host = email_host;
        this.email_port = email_port;
        this.Subject = RealTimePriceAlertSubject;
        this.MessageBody = RealTimePriceAlertMessage;
        this.FilePath = RealTimePriceAlertFilePath;
        this.FileName = RealTimePriceAlertFileName;
        
    }

    public void send() 
    {
      // Get system properties
      Properties properties = System.getProperties();

      // Setup mail server
      properties.put("mail.smtp.port", email_port);
      properties.setProperty("mail.smtp.host", email_host);
      properties.put("mail.smtp.auth", false);

      // Get the default Session object.
      Session session = Session.getInstance(properties);

      try{
         // Create a default MimeMessage object.
         MimeMessage message = new MimeMessage(session);

         // Set From: header field of the header.
         message.setFrom(new InternetAddress(FROM));

         // Set To: header field of the header.
         for(String emails1 : emails)
         {
             System.out.println(emails1);
             message.addRecipient(Message.RecipientType.TO,
                                  new InternetAddress(emails1));
         }
         
         // Set Subject: header field
         message.setSubject(Subject);

         // Create the message part 
         BodyPart messageBodyPart = new MimeBodyPart();

         // Fill the message
         messageBodyPart.setText(MessageBody);
         
         // Create a multipar message
         Multipart multipart = new MimeMultipart();

         // Set text message part
         multipart.addBodyPart(messageBodyPart);

         // Part two is attachment
         messageBodyPart = new MimeBodyPart();
         DataSource source = new FileDataSource(FilePath);
         messageBodyPart.setDataHandler(new DataHandler(source));
         messageBodyPart.setFileName(FileName);
         multipart.addBodyPart(messageBodyPart);

         // Send the complete message parts
         message.setContent(multipart );

         // Send message
         Transport.send(message);
         System.out.println("Sent message successfully....");
      }catch (MessagingException mex) {
          System.out.println("SendEmailAttachment Failed: " + mex);
      }
   }
    
}
