package com.logic4nil.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MemoryTable extends AbstractTable implements ScannableTable {

    private final Type elementType;
    private final Enumerable enumerable;
    private final List datas;

    public MemoryTable(Type elementType, List datas) {
        this.elementType = elementType;
        this.datas = datas;
        this.enumerable = Linq4j.asEnumerable(datas);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        if(((Class)elementType).isAssignableFrom(Map.class)){
            if(datas.size() <= 0){
                throw new RuntimeException("The Length Of List<Map> Must greater than 0");
            }
            Map<String, Object> data = (Map<String, Object>) datas.get(0);
            for(String key: data.keySet().stream().sorted().collect(Collectors.toList())){
                names.add(key);
                types.add(typeFactory.createJavaType(data.get(key).getClass()));
            }
            return typeFactory.createStructType(Pair.zip(names, types));
        } else {
            return ((JavaTypeFactory) typeFactory).createType(elementType);
        }
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        if (elementType == Object[].class) {
            //noinspection unchecked
            return enumerable;
        } else if (((Class)elementType).isAssignableFrom(Map.class)) {
            // Element Type is Map
            JavaTypeFactory typeFactory = root.getTypeFactory();
            List<RelDataTypeField> fieldTypes = getRowType(typeFactory).getFieldList();
            List<String> fieldNames = fieldTypes.stream().map(fieldType -> fieldType.getName()).collect(Collectors.toList());

            return enumerable.select(new Function1() {

                @Override
                public Object[] apply(Object o) {
                    int size = fieldNames.size();
                    final Object[] objects = new Object[size];
                    for(int i = 0; i < size; i++) {
                        objects[i] = ((Map)o).get(fieldNames.get(i));
                    }
                    return objects;
                }
            });
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
}
