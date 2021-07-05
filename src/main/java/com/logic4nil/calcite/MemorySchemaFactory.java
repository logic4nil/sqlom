package com.logic4nil.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class MemorySchemaFactory implements SchemaFactory {
    public static final MemorySchemaFactory INSTANCE = new MemorySchemaFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

        MemorySchema schema = new MemorySchema();
        parentSchema.add(name, schema);

        return schema;
    }

    public static Schema create(Connection connection, String schemName) throws SQLException {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        return INSTANCE.create(rootSchema, schemName, ImmutableMap.of());
    }
}
