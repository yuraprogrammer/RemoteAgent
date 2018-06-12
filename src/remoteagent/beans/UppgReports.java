package remoteagent.beans;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import static remoteagent.beans.AgentBase.dbData;

/**
 *
 * @author yura_
 */
public class UppgReports extends AgentBase{
    private boolean shiftStarted;
    private int shift, prevShift;
    private Date prevActDate = new Date();
    private BigDecimal[] counterValues;
    
    private void createAct(long newActId, Date d1, int shift) throws SQLException{
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String newAct = "insert into Act_UPPG (id, aDate, aShift, mainOper, slaveOper, Complete, aCode) values ("+
                        String.valueOf(newActId)+",'"+dateFormat.format(d1)+"',"+String.valueOf(shift)+","+String.valueOf(100)+","+
                        String.valueOf(100)+","+String.valueOf(0)+","+String.valueOf(0)+")";
        Statement stm = dbData.db.createStatement();
        stm.executeUpdate(newAct);
        stm.close();
    }
    
    private void createCounters(long newActId, long id, BigDecimal[] data) throws SQLException{
        String newAct = "insert into Act_Counters (id, actID, MassStart_S, VolumeStart_S, DensityStart_S, TempStart_S,"
                                    + "MassStart_B, VolumeStart_B, DensityStart_B, TempStart_B, MassStart_A, VolumeStart_A, DensityStart_A, TempStart_A,"
                                    + "MassEnd_S, VolumeEnd_S, DensityEnd_S, TempEnd_S, MassEnd_B, VolumeEnd_B, DensityEnd_B, TempEnd_B,"
                                    + "MassEnd_A, VolumeEnd_A, DensityEnd_A, TempEnd_A) values ("+String.valueOf(id)+","+String.valueOf(newActId)+String.valueOf(data[0])+","
                                    + String.valueOf(data[1])+","+String.valueOf(data[1])+"," +String.valueOf(data[2])+","+String.valueOf(data[3])+","+String.valueOf(data[4])+","
                                    + String.valueOf(data[5])+","+String.valueOf(data[6])+","+String.valueOf(data[7])+","+String.valueOf(data[8])+","+String.valueOf(data[9])+","
                                    + String.valueOf(data[10])+","+String.valueOf(data[11])+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(newAct);
        stm.close();
    }
    
    private void createDensity20(long newActId, long id) throws SQLException{
        String newAct = "insert into Act_Density20 (id, actID) values ("+String.valueOf(id)+","+String.valueOf(newActId)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(newAct);
        stm.close();
    }
    
    private void createSirie(long newActId, long id) throws SQLException{
        String newAct = "insert into Act_Sirie (id, actID) values ("+String.valueOf(id)+","+String.valueOf(newActId)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(newAct);
        stm.close();
    }
    
    private void createOtgToTsp(long actID) throws SQLException{
        Statement stm = dbData.db.createStatement();
        for (int i=1; i<4; i++){
            long id = getNewId("OTG_To_TSP");
            String query = "insert into dbo.UPPG_Drain_Tank (id, actID, tankOrder) values ("+String.valueOf(id)+","+String.valueOf(actID)+","+String.valueOf(i)+")";
            stm.execute(query);
        }
        stm.close();
    }
    
    private void createOtgToUppg(long actID, long id) throws SQLException{
        String query = "insert into dbo.UPPG_Drain_Tank (id, actID) values ("+String.valueOf(id)+","+String.valueOf(actID)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(query);
        stm.close();
    }
    
    private void createDrainTank(long actID, long id) throws SQLException{
        String query = "insert into dbo.UPPG_Drain_Tank (id, actID) values ("+String.valueOf(id)+","+String.valueOf(actID)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(query);
        stm.close();
    }
    
    private void createFeedWater(long actID, long id) throws SQLException{
        String query = "insert into dbo.UPPG_Feed_Water (id, actID) values ("+String.valueOf(id)+","+String.valueOf(actID)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(query);
        stm.close();
    }
    
    private void createSirieMixing(long actID, long id) throws SQLException{
        String query = "insert into dbo.Act_SirieMixing (id, actID) values ("+String.valueOf(id)+","+String.valueOf(actID)+")";
        Statement stm = dbData.db.createStatement();
        stm.execute(query);
        stm.close();
        
    }
    
    private long getNewId(String table) throws SQLException{
        long id=0;
        String newAct = "select MAX(id) from "+table;
        Statement stm = dbData.db.createStatement();
        ResultSet rs = stm.executeQuery(newAct);
        if (rs.next()){
            id = rs.getLong(1)+1;
        }
        stm.close();
        return id;
    }
       
    private long getPrevActId(){
        long id = 0;
        try {
            Statement stm = dbData.db.createStatement();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String sql = "select id from dbo.Act_UPPG where aDate='"+dateFormat.format(prevActDate)+"' and aShift="+String.valueOf(prevShift);
            ResultSet rs = stm.executeQuery(sql);
            if (rs.next()){
                id = rs.getLong("id");
            }
        } catch (SQLException ex) {
            Logger.getLogger(UppgReports.class.getName()).log(Level.SEVERE, null, ex);
        }
        return id;
    }
    
    private BigDecimal getTagValue(String date, int shift, String tag_name){        
        BigDecimal value=BigDecimal.ZERO;
        try {
            Statement stmt = dbData.db.createStatement();
            String query = "SELECT tag_value FROM dbo.counters_daq WHERE daq_dt = :'"+date+"' AND shift = :"+String.valueOf(shift)+" AND tag_name = :'"+tag_name+"'";
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()){
                value = rs.getBigDecimal(1);
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            Logger.getLogger(UppgReports.class.getName()).log(Level.SEVERE, null, ex);
        }
        return value;
    }
    
    private BigDecimal getDensity20(Double density, Double temperature){
        BigDecimal density20=BigDecimal.ZERO;
        try {
            
            String str = "SELECT plotn20 FROM v_plotn20 WHERE plotn like '"+String.format("%.3f", density).replace(",", ".")+"%' AND temper_name = "+String.format("%d", Math.round(temperature));
            Statement stmt = dbData.db.createStatement();
            ResultSet rs = stmt.executeQuery(str);
            if (rs.next()){
                density20 = BigDecimal.valueOf(Double.parseDouble(rs.getString(1).replace(",", ".")));
            }
        } catch (SQLException ex) {
            Logger.getLogger(UppgReports.class.getName()).log(Level.SEVERE, null, ex);
        }
        return density20;
    }
    
    private void savePrevCounters(String prevDate, int prevShift, long actID) throws SQLException{
        String sql = "select * from dbo.counters_daq where daq_dt='"+prevDate+"' and shift="+String.valueOf(prevShift)+" order by id";
        Statement stm = dbData.db.createStatement();
        ResultSet rs = stm.executeQuery(sql);
        ArrayList<BigDecimal> values;
        values = new ArrayList();
        while (rs.next()){
            values.add(rs.getBigDecimal("tag_value"));
        }
        if (!values.isEmpty()){
            //Сохраняем данные счетчиков на конец предыдущей смены и начало новой
            counterValues = new BigDecimal[12];
            for (int i=0; i<12; i++){
                counterValues[i]=values.get(i);
            }
            String updateCounters = "update dbo.Act_Counters set MassEnd_S="+String.valueOf(values.get(0))+
                                    ",VolumeEnd_S="+String.valueOf(values.get(1))+
                                    ",DensityEnd_S="+String.valueOf(values.get(2))+
                                    ",TempEnd_S="+String.valueOf(values.get(3))+
                                    ",MassEnd_B="+String.valueOf(values.get(4))+
                                    ",VolumeEnd_B="+String.valueOf(values.get(5))+
                                    ",DensityEnd_B="+String.valueOf(values.get(6))+
                                    ",TempEnd_B="+String.valueOf(values.get(7))+
                                    ",MassEnd_A="+String.valueOf(values.get(8))+
                                    ",VolumeEnd_A="+String.valueOf(values.get(9))+
                                    ",DensityEnd_A="+String.valueOf(values.get(10))+
                                    "TempEnd_A="+String.valueOf(values.get(11))+
                                    " where actID="+String.valueOf(actID);
            stm.executeUpdate(updateCounters);
        }
        stm.clearWarnings();
    }
    
    @Override
    public void doTask(){
    //Определение начала новой смены
        Date d1 = new Date();       //Текущие дата и время
        Date d2 = new Date();       //Дата и время первой смены сегодня
        Date d3 = new Date();       //Дата и время второй смены сегодня
        d2.setHours(8);             
        d2.setMinutes(0);
        d2.setSeconds(0);
        d3.setHours(20);
        d3.setMinutes(0);
        d3.setSeconds(0); 
        int cnt = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");                                 
        
        //Определение номера смены    
        if (d1.after(d2) && d1.before(d3)){
            shift=1;
        }else{
            shift=2;
        }            
            
        int currentHour = d1.getHours();
        if (currentHour>=0 && currentHour<=7){
            int curDay = d1.getDate();
            int yesterday = curDay-1;
            d1.setDate(yesterday);                        
        }
        try{ //Определение повторного запуска операций для текущей смены
            Statement stm = dbData.db.createStatement(); 
            String sql="select count(id) from dbo.Act_UPPG where aDate = '"+dateFormat.format(d1)+"' and shift="+String.valueOf(shift);                
            ResultSet rs = stm.executeQuery(sql);
            while (rs.next()){
                cnt = rs.getInt(1);
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(OilAccount.class.getName()).log(Level.SEVERE, ex.getMessage());
            getDbConnection();
            getTask();
        }
        //Определение признака начала новой смены              
        shiftStarted = cnt==0;
        //Если началась новая смена - заполняем данные на конец предыдущей смены и используем их на начало новой смены
        if (shiftStarted){                                                                   
            if (shift==1){
                int curDay = d1.getDate();
                int yesterday = curDay-1;
                prevActDate.setDate(yesterday);
                prevShift=2;
            }else{
                prevActDate=d1;
                prevShift=1;
            }
            try {
                //Находим id акта за предыдущую смену
                long prevId = getPrevActId();
                if (prevId!=0){
                    savePrevCounters(dateFormat.format(prevActDate), prevShift, prevId);
                    //Создаем новый акт и используем в нем все данные на конец предыдущей смены
                    long newActId=getNewId("Act_UPPG");  
                    if (newActId!=0){
                        createAct(newActId, d1, shift);    
                        createCounters(newActId, getNewId("Act_Couunters"), counterValues);
                        createSirie(getNewId("Act_Sirie"), newActId);
                        createSirieMixing(getNewId("Act_SirieMixing"), newActId);
                        createOtgToTsp(newActId);
                        createOtgToUppg(getNewId("OTG_To_UPPG"), newActId);
                        createFeedWater(getNewId("UPPG_Feed_Water"), newActId);
                        createDrainTank(getNewId("UPPG_Drain_Tank"), newActId);
                        createDensity20(getNewId("Act_Density20"), newActId);
                    }
                }
            } catch (SQLException ex) {
                Logger.getLogger(UppgReports.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
