package com.logic4nil.calcite;

import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MemoryFactoryTest {

    public static class People {
        // field
        public String id;
        public String name;
        public People(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }
    // table
    public static class Detail {
        // field
        public String id;
        public int age;

        public Detail(String id, int age) {
            this.id = id;
            this.age = age;
        }
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i < columnCount + 1; i++) {
                row.add(resultSet.getObject(i));
            }
            System.out.println(row);
        }
    }

    @Test
    void testGetConnection() throws SQLException {
        assertEquals(MemoryFactory.getConnection(true), MemoryFactory.getConnection(true));
        assertNotEquals(MemoryFactory.getConnection(true), MemoryFactory.getConnection(false));
    }

    @Test void testMemoryAll() throws SQLException {

        Connection con = MemoryFactory.getConnection(true);

        Schema schema = MemorySchemaFactory.create(con, "sc");

        People[] people = new People[]{
                new People("1", "namea"),
                new People("2", "nameb"),
                new People("3", "namec")
        };
        Detail[] detail = new Detail[]{
                new Detail("1", 1),
                new Detail("2", 22),
                new Detail("3", 333)
        };
        ((MemorySchema) schema).addCollectionDatas(People.class, Arrays.asList(people));
        ((MemorySchema) schema).addCollectionDatas(Detail.class, Arrays.asList(detail));
        // 创建 Statement
        Statement statement = con.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from sc.detail");
        printResultSet(resultSet);
        resultSet.close();
        statement.close();
        con.close();
    }

    @Test void testMap() throws SQLException {
        Connection con = MemoryFactory.getConnection(true);

        Schema schema = MemorySchemaFactory.create(con, "sc");
        ArrayList list = new ArrayList();

        HashMap<String, Object> one = new HashMap<String, Object>();
        one.put("field1", 123);
        one.put("field2", "test");
        list.add(one);
        HashMap<String, Object> one2 = new HashMap<String, Object>();
        one2.put("field1", 12);
        one2.put("field2", "test1");
        list.add(one2);

        ((MemorySchema) schema).addMapDatas("test", list);

        Statement statement = con.createStatement();

        ResultSet resultSet = statement.executeQuery("select max(field1) from sc.test");
        printResultSet(resultSet);
        resultSet.close();
        statement.close();
        con.close();
    }
}