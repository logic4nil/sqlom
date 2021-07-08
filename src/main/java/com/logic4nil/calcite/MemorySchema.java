package com.logic4nil.calcite;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;

public class MemorySchema extends AbstractSchema {

    private Map<String, Table> tableMaps = new HashMap<String, Table>() ;

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMaps;
    }

    public void addTable(String tableName, MemoryTable table){
        tableMaps.put(tableName, table);
    }

    public void addCollectionDatas(Class<?> clazz, List data){
        this.addTable(clazz.getSimpleName().toLowerCase(Locale.getDefault()), new MemoryTable(clazz, data));
    }

    public void addMapDatas(String tableName, List<Map> data){
        this.addTable(tableName.toLowerCase(Locale.getDefault()), new MemoryTable(Map.class, data));
    }

    private static Enumerable toEnumerable(final Object o) {
        if (o.getClass().isArray()) {
            if (o instanceof Object[]) {
                return Linq4j.asEnumerable((Object[]) o);
            } else {
                return Linq4j.asEnumerable(Primitive.asList(o));
            }
        }
        if (o instanceof Iterable) {
            return Linq4j.asEnumerable((Iterable) o);
        }
        throw new RuntimeException(
                "Cannot convert " + o.getClass() + " into a Enumerable");
    }
}
