package com.logic4nil.calcite;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class MemorySchema extends AbstractSchema {

    private Map<String, Table> tableMaps = new HashMap<String, Table>() ;

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMaps;
    }

    public void addTable(String tableName, Table table){
        tableMaps.put(tableName, table);
    }

    public void addTable(Class<?> clazz, Table table) {
        this.addTable(clazz.getSimpleName().toLowerCase(Locale.ROOT), table);
    }

    public void addCollectionDatas(Class<?> clazz, Collection data){
        this.addTable(clazz, new MemoryTable(clazz, toEnumerable(data)));
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
