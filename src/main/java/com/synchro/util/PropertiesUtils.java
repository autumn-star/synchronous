package com.synchro.util;

import java.util.Locale;
import java.util.ResourceBundle;

public class PropertiesUtils {
    public static String getProperties(String key) {
        // TODO Auto-generated method stub
        ResourceBundle rb=ResourceBundle.getBundle("sync", Locale.CHINA);
        return rb.getString(key);
    }
    
    public static void main(String[] args){
    	String value = PropertiesUtils.getProperties("tempFileFold");
    	System.out.println(value);
    }

}