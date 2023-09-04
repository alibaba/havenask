package com.taobao.search.iquan.core.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.taobao.search.iquan.core.common.ConstantDefine;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class IquanSimpleCalciteSchema extends CalciteSchema {
    IquanSimpleCalciteSchema(@Nullable CalciteSchema parent, Schema schema, String name) {
        this(parent, schema, name, null, null, null, null, null, null, null, null);
    }

    protected IquanSimpleCalciteSchema(@Nullable CalciteSchema parent,
                                       Schema schema,
                                       String name,
                                       @Nullable NameMap<CalciteSchema> subSchemaMap,
                                       @Nullable NameMap<TableEntry> tableMap,
                                       @Nullable NameMap<LatticeEntry> latticeMap,
                                       @Nullable NameMap<TypeEntry> typeMap,
                                       @Nullable NameMultimap<FunctionEntry> functionMap,
                                       @Nullable NameSet functionNames,
                                       @Nullable NameMap<FunctionEntry> nullaryFunctionMap,
                                       @Nullable List<? extends List<String>> path) {
        super(parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path);
    }

    private static @Nullable String caseInsensitiveLookup(Set<String> candidates, String name) {
        // Exact string lookup
        if (candidates.contains(name)) {
            return name;
        }
        // Upper case string lookup
        final String upperCaseName = name.toUpperCase(Locale.ROOT);
        if (candidates.contains(upperCaseName)) {
            return upperCaseName;
        }
        // Lower case string lookup
        final String lowerCaseName = name.toLowerCase(Locale.ROOT);
        if (candidates.contains(lowerCaseName)) {
            return lowerCaseName;
        }
        // Fall through: Set iteration
        for (String candidate: candidates) {
            if (candidate.equalsIgnoreCase(name)) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        // Check implicit schemas.
        final String schemaName2 = caseSensitive ? schemaName : caseInsensitiveLookup(
                schema.getSubSchemaNames(), schemaName);
        if (schemaName2 == null) {
            return null;
        }
        final Schema s = schema.getSubSchema(schemaName2);
        if (s == null) {
            return null;
        }
        return new IquanSimpleCalciteSchema(this, s, schemaName2);
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        if (isTemplateName(tableName)) {
            final Table table = schema.getTable(tableName);
            return table == null ? null : tableEntry(tableName, table);
        }
        final String tableName2 = caseSensitive ? tableName : caseInsensitiveLookup(
                schema.getTableNames(), tableName);
        if (tableName2 == null) {
            return null;
        }
        final Table table = schema.getTable(tableName2);
        if (table == null) {
            return null;
        }
        return tableEntry(tableName2, table);
    }

    @Override
    protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
        // Check implicit types.
        final String name2 = caseSensitive ? name : caseInsensitiveLookup(
                schema.getTypeNames(), name);
        if (name2 == null) {
            return null;
        }
        final RelProtoDataType type = schema.getType(name2);
        if (type == null) {
            return null;
        }
        return typeEntry(name2, type);
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
        Collection<Function> functions = schema.getFunctions(tableName);
        if (functions != null) {
            for (Function function : functions) {
                if (function instanceof TableMacro
                        && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    return tableEntry(tableName, table);
                }
            }
        }
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
        ImmutableSortedMap<String, CalciteSchema> explicitSubSchemas = builder.build();
        for (String schemaName : schema.getSubSchemaNames()) {
            if (explicitSubSchemas.containsKey(schemaName)) {
                // explicit subschema wins.
                continue;
            }
            Schema s = schema.getSubSchema(schemaName);
            if (s != null) {
                CalciteSchema calciteSchema = new IquanSimpleCalciteSchema(this, s, schemaName);
                builder.put(schemaName, calciteSchema);
            }
        }
    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getTableNames());
    }

    @Override
    protected void addImplicitFunctionsToBuilder(ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {
        Collection<Function> functions = schema.getFunctions(name);
        if (functions != null) {
            builder.addAll(functions);
        }
    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getFunctionNames());
    }

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getTypeNames());
    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(ImmutableSortedMap.Builder<String, Table> builder) {
        ImmutableSortedMap<String, Table> explicitTables = builder.build();

        for (String s : schema.getFunctionNames()) {
            // explicit table wins.
            if (explicitTables.containsKey(s)) {
                continue;
            }
            for (Function function : schema.getFunctions(s)) {
                if (function instanceof TableMacro
                        && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    builder.put(s, table);
                }
            }
        }
    }

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
        IquanSimpleCalciteSchema snapshot = new IquanSimpleCalciteSchema(parent,
                schema.snapshot(version), name, null, tableMap, latticeMap, typeMap,
                functionMap, functionNames, nullaryFunctionMap, getPath());
        for (CalciteSchema subSchema : subSchemaMap.map().values()) {
            CalciteSchema subSchemaSnapshot = ((IquanSimpleCalciteSchema) subSchema).snapshot(snapshot, version);
            snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot);
        }
        return snapshot;
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

    @Override
    public void setCache(boolean cache) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        final CalciteSchema calciteSchema =
                new IquanSimpleCalciteSchema(this, schema, name);
        subSchemaMap.put(name, calciteSchema);
        return calciteSchema;
    }

    private static boolean isTemplateName(String name) {
        return name != null && name.contains(ConstantDefine.TEMPLATE_TABLE_SEPARATION);
    }

    public static IquanSimpleCalciteSchema createRootSchema(String name, Schema schema) {
        return new IquanSimpleCalciteSchema(null, schema, name);
    }
}
