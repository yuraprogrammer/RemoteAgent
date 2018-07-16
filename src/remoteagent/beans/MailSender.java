/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteagent.beans;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import remoteagent.lib.BooleanTag;
import remoteagent.lib.SimaticAddressParser;

/**
 *
 * @author yura_
 */
public class MailSender extends AgentBase{
    private String USER_NAME;
    private String PASSWORD;
    private String RECIPIENT;
    String from;
    String pass;
    String[] to;
    String subject;
    String body;
    String host;
    String port;
    
    private JPlcAgent plc[];
    private SimaticAddressParser tags[];
    private BooleanTag plcTags[];
    private NodeList paramList;
    private NodeList plcList;
    private NodeList addresses;
    private NodeList senders;
    private boolean oldAlarm, newAlarm;
    
    public MailSender() {
        
    }
        
    @Override
    public void getTask(){
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = null;
            try {
                db = dbf.newDocumentBuilder();
            } catch (ParserConfigurationException ex) {
                System.out.println("Unable create document!!!");
            }
            Document doc = null;
            File xmlFile = new File(System.getProperty("user.dir")+File.separator+"alarmTags.xml");
            doc = db.parse(xmlFile);
                    
            Node node = doc.getChildNodes().item(0);
            
            paramList = doc.getElementsByTagName("PlcTag");
            plcTags = new BooleanTag[paramList.getLength()];
            for (int i=0; i<paramList.getLength(); i++){
                plcTags[i] = new BooleanTag();
                NamedNodeMap attributes = paramList.item(i).getAttributes();
                Node nameAttrib = attributes.getNamedItem("name");
                plcTags[i].setTagName(nameAttrib.getNodeValue());
                Node plcAttrib = attributes.getNamedItem("plcName");
                plcTags[i].setPlcName(plcAttrib.getNodeValue());
                Node addrAttrib = attributes.getNamedItem("S7Addr");
                plcTags[i].setS7Addr(addrAttrib.getNodeValue());                                
            }
            tags = new SimaticAddressParser[paramList.getLength()];
                for (int i=0; i<paramList.getLength(); i++){
                    tags[i] = new SimaticAddressParser(plcTags[i].getS7Addr());
                }
            plcList = doc.getElementsByTagName("PLC");
            plc = new JPlcAgent[plcList.getLength()];
            for (int i=0; i<plcList.getLength(); i++){
                NamedNodeMap attributes = plcList.item(i).getAttributes();
                Node plcAttrib = attributes.getNamedItem("host");
                Node nameAttrib = attributes.getNamedItem("name");
                plc[i] = new JPlcAgent(plcAttrib.getNodeValue(), nameAttrib.getNodeValue(), i+1);
            }
            senders = doc.getElementsByTagName("Sender");
            Node sender = senders.item(0);
            NamedNodeMap senderAttr = sender.getAttributes();
            USER_NAME = senderAttr.getNamedItem("name").getNodeValue();
            PASSWORD = senderAttr.getNamedItem("password").getNodeValue();
            host = senderAttr.getNamedItem("host").getNodeValue();
            port = senderAttr.getNamedItem("port").getNodeValue();
            this.pass = PASSWORD;
            this.from = USER_NAME;
            
            addresses = doc.getElementsByTagName("Reciever");
            to = new String[addresses.getLength()];
            for (int i=0; i<addresses.getLength(); i++){
                NamedNodeMap attributes = addresses.item(i).getAttributes();
                Node nameAttrib = attributes.getNamedItem("name");
                to[i] = nameAttrib.getNodeValue();
            }
            
        } catch (SAXException | IOException ex) {
            Logger.getLogger(DAQ_And_Store.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void doTask(){
        while (true){
            newAlarm = isAlarm();
            if (true == newAlarm!=oldAlarm){
                this.body = "Аварийное значение давления газа!!!";
                this.subject = "Авария процесса";
                sendMail(from, pass, to, subject, body);
                oldAlarm=newAlarm;
            }    
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(MailSender.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private void sendMail(String from, String pass, String[] to, String subject, String body) {
        Properties props = System.getProperties();
        
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.user", from);
        props.put("mail.smtp.password", pass);
        props.put("mail.smtp.port", port);
        props.put("mail.smtp.auth", "true");

        Session session = Session.getDefaultInstance(props);
        MimeMessage message = new MimeMessage(session);

        try {
            message.setFrom(new InternetAddress(from));
            InternetAddress[] toAddress = new InternetAddress[to.length];

            // To get the array of addresses
            for( int i = 0; i < to.length; i++ ) {
                toAddress[i] = new InternetAddress(to[i]);
            }

            for( int i = 0; i < toAddress.length; i++) {
                message.addRecipient(Message.RecipientType.TO, toAddress[i]);
            }

            message.setSubject(subject, "UTF-8");
            message.setText(body, "UTF-8");
            Transport transport = session.getTransport("smtp");
            transport.connect(host, from, pass);
            transport.sendMessage(message, message.getAllRecipients());
            System.out.println(message.getSubject());
            transport.close();
        }
        catch (AddressException ae) {
            ae.printStackTrace();
        }
        catch (MessagingException me) {
            me.printStackTrace();
        }
    }
    
    private boolean isAlarm(){
        boolean alarm = false;
        int count = paramList.getLength();
        if (count!=0){
            for (int i=0; i<count; i++){
                for (int j=0; j<plcList.getLength(); j++){
                        plcTags[i].setCurrentValue(plc[j].readBooleanData(tags[i].getDbNum(), tags[i].getStartAddress(), tags[i].getBitNum()));
                        alarm = plcTags[i].isCurrentValue();
                }
            }
        }
        return alarm;
    }
}
