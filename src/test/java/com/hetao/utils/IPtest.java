package com.hetao.utils;


import org.junit.Test;

public class IPtest {

    @Test
    public void testIP(){
        IPParser.RegionInfo regionInfo=IPParser.getInstance().analyseIp("113.119.73.206");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }
}
