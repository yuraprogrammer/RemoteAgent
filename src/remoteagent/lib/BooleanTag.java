/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package remoteagent.lib;

/**
 *
 * @author yura_
 */
public class BooleanTag {
    private String tagName = "NewTag";
    private String plcName = "BOOL_1";

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getPlcName() {
        return plcName;
    }

    public void setPlcName(String plcName) {
        this.plcName = plcName;
    }

    public String getS7Addr() {
        return S7Addr;
    }

    public void setS7Addr(String S7Addr) {
        this.S7Addr = S7Addr;
    }

    public boolean isCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(boolean currentValue) {
        this.currentValue = currentValue;
    }
    private String S7Addr = "DB1.DBX0.0";
    private boolean currentValue;
    
    public BooleanTag(){
        
    }
    
    public BooleanTag(String tagName, String plcName, String S7Addr){
        setTagName(tagName);
        setPlcName(plcName);
        setS7Addr(S7Addr);
    }
}
