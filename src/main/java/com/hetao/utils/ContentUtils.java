package com.hetao.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentUtils {
    public  static  String  getPageId(String url){
        String pageId="";
        if (StringUtils.isBlank(url)){
            return pageId;
        }
        Pattern pattern=Pattern.compile("topicId=[0-9]+");
        Matcher matcher=pattern.matcher(url);

        if(matcher.find()){
            pageId=matcher.group().split("topicId=")[1];
        }
        return  pageId;
    }

//    public static String getIP(String ip){
//        String userIp="-";
//
//        if(StringUtils.isNotBlank(ip)){
//            Pattern pattern=Pattern.compile(ip);
//            Matcher matcher=pattern.matcher(ip);
//
//            if (matcher.find()){
//                userIp=ip;
//            }
//            return userIp;
//        }
//
//        return userIp;
//    }

    public  static  String  getos(String url){
        String os="";
        if (StringUtils.isBlank(url)){
            return os;
        }
        Pattern pattern=Pattern.compile("Windows NT [1-9].[1-9]+");
        Matcher matcher=pattern.matcher(url);
        if(matcher.find()){
           os="Windows";
        }else{
            pattern=Pattern.compile("Android [1-9].[1-9].[1-9]");
            matcher=pattern.matcher(url);
            if(matcher.find()){
                os="Android";
            }else {
                pattern=Pattern.compile("Mac OS X");
                matcher=pattern.matcher(url);
                if(matcher.find()){
                    os="IPhone/IPad";
                }else{
                    os="Other";
                }

            }
        }
        return  os;
    }
}
