package com.mdb.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveClientDemo {

    private static String commonDriverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://erie3.example.com:2181,erie1.example.com:2181,erie4.example.com:2181,erie2.example.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static String userName = "hive";
    private static String userPass = "hive";
    public static void main(String[] args) throws Exception {
        Class.forName(commonDriverName);
        Connection con = DriverManager.getConnection(url, userName, userPass);
        System.out.println("\n\t Got Connection: " + con);

            System.out.println("*** List the existing Databases....");
        Statement stmt = con.createStatement();
            String sql = "show databases";
            System.out.println("Executing Query: " + sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                  System.out.println(rs.getString(1));
            }

            sql = "create database Test12345DB";
            System.out.println("Executing Query: " + sql);
            stmt.execute(sql);

            System.out.println("*** After Creating a new Database....");
            sql = "show databases";
            System.out.println("Executing Query: " + sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                  System.out.println(rs.getString(1));
            }
    }
}