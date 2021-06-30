package com.logic4nil.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MemorySchemaFactory implements SchemaFactory {
    public static final MemorySchemaFactory INSTANCE = new MemorySchemaFactory();
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new MemorySchema();
    }
}
