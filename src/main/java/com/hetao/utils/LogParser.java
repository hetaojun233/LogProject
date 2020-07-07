package com.hetao.utils;


import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser {
    public Map<String,String> parse(String log){
        IPParser ipParser=IPParser.getInstance();

        Map<String,String> info=new HashMap<>();
        if(StringUtils.isNotBlank(log)){
            String[] splits=log.split("\001");

            String ip=splits[13];

            String country ="-";
            String province ="-";
            String city ="-";
            IPParser.RegionInfo regionInfo=ipParser.analyseIp(ip);

            if(regionInfo!=null){
                country=regionInfo.getCountry();
                province=regionInfo.getProvince();
                city=regionInfo.getCity();
            }


            info.put("ip",ip);
            info.put("country",country);
            info.put("province",province);
            info.put("city",city);


        }

        return info;
    }

}