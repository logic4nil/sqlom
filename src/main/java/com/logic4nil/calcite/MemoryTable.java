package com.logic4nil.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

public class MemoryTable extends AbstractQueryableTable implements ScannableTable {

    private final Enumerable enumerable;

    public MemoryTable(Type elementType, Enumerable enumerable) {
        super(elementType);
        this.enumerable = enumerable;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return ((JavaTypeFactory) typeFactory).createType(elementType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        if (elementType == Object[].class) {
            //noinspection unchecked
            return enumerable;
        } else {
            //noinspection unchecked
            return enumerable.select(new Function1() {
                @Override public @Nullable Object[] apply(Object o) {
                    Field[] fields = ((Class) elementType).getFields();
                    try {
                        final @Nullable Object[] objects = new Object[fields.length];
                        for (int i = 0; i < fields.length; i++) {
                            objects[i] = fields[i].get(o);
                        }
                        return objects;
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this,
                tableName) {
            @SuppressWarnings("unchecked")
            @Override public Enumerator<T> enumerator() {
                return (Enumerator<T>) enumerable.enumerator();
            }
        };
    }
}
