package com.taobao.search.iquan.core.common;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public interface ConstantDefine {

    // attr name: common
    String ID = "id";
    String OP = "op";
    String OP_NAME = "op_name";
    String OP_SCOPE = "op_scope";
    String NAME = "name";
    String INPUTS = "inputs";
    String INPUT = "input";
    String LEFT = "input0";
    String RIGHT = "input1";
    String REUSE_INPUTS = "reuse_inputs";
    String OUTPUT = "output";
    String OUTPUTS = "outputs";
    String TYPE = "type";
    String PARAMS = "params";
    String PARAM_TYPES = "param_types";
    String FIELD_NAME = "field_name";
    String FIELD_TYPE = "field_type";
    String INDEX_NAME = "index_name";
    String INDEX_TYPE = "index_type";
    String REX_VARIABLE = "rex_variable";
    String REX_ACCESS_FIELD = "rex_access_field";
    String FIELD_META = "field_meta";
    String TABLE_META = "table_meta";
    String PK_FIELDS = "pk_fields";
    String PK_FIELD = "pk_field";
    String IS_ATTRIBUTE = "is_attribute";

    String OPERATION = "operation";
    String OUTPUT_FIELDS = "output_fields";
    String OUTPUT_FIELDS_INTERNAL = "output_fields_internal";
    String OUTPUT_FIELDS_TYPE = "output_fields_type";
    String OUTPUT_FIELDS_INTERNAL_TYPE = "output_fields_internal_type";
    String OUTPUT_FIELD_EXPRS = "output_field_exprs";
    String MODIFY_FIELD_EXPRS = "modify_field_exprs";
    String OUTPUT_FIELDS_HASH = "output_fields_hash";
    String MATCHDOCS_EXPRS = "matchdocs_exprs";
    String TO_TENSOR_EXPRS = "to_tensor_exprs";
    String LEFT_INPUT_FIELDS = "left_input_fields";
    String RIGHT_INPUT_FIELDS = "right_input_fields";
    String INPUT_FIELDS = "input_fields";
    String INPUT_FIELDS_TYPE = "input_fields_type";
    String CONDITION = "condition";
    String JOIN_LEFT_KEY = "join_left_key";
    String JOIN_RIGHT_KEY = "join_rigth_key";
    String INVOCATION = "invocation";
    String DISTRIBUTION = "distribution";
    String TOP_DISTRIBUTION = "top_distribution";
    String TOP_PROPERTIES = "top_properties";
    String LEFT_IS_BUILD = "left_is_build";
    String IS_BROADCAST = "is_broadcast";
    String TRY_DISTINCT_BUILD_ROW = "try_distinct_build_row";
    String IS_INTERNAL_BUILD = "is_internal_build";
    String IS_EQUI_JOIN = "is_equi_join";
    String EQUI_CONDITION = "equi_condition";
    String REMAINING_CONDITION = "remaining_condition";

    String CATALOG_NAME = "catalog_name";
    String DB_NAME = "db_name";
    String TABLE_NAME = "table_name";
    String AUX_TABLE_NAME = "aux_table_name";
    String TABLE_TYPE = "table_type";
    String TABLE_DISTRIBUTION = "table_distribution";
    String OUTPUT_DISTRIBUTION = "output_distribution";
    String OUTPUT_PRUNABLE = "output_prunable";
    String LOCATION = "location";
    String EQUAL_HASH_FIELDS = "equal_hash_fields";
    String PART_FIX_FIELDS = "part_fix_fields";
    String HASH_MODE = "hash_mode";
    String HASH_TYPE = "hash_type";
    String HASH_FUNCTION = "hash_function";
    String HASH_FIELDS = "hash_fields";
    String HASH_PARAMS = "hash_params";
    String PARTITION_CNT = "partition_cnt";
    String HASH_VALUES = "hash_values";
    String PK_VALUES = "pk_values";
    String HASH_VALUES_SEP = "hash_values_sep";
    String PK_VALUES_SEP = "pk_values_sep";
    String ASSIGNED_PARTITION_IDS = "assigned_partition_ids";
    String ASSIGNED_HASH_VALUES = "assigned_hash_values";

    // attr name
    String LEFT_TABLE_META = "left_table_meta";
    String RIGHT_TABLE_META = "right_table_meta";
    String USED_FIELDS = "used_fields";
    String USED_FIELDS_TYPE = "used_fields_type";
    String KEYS = "keys";
    String LIMIT = "limit";
    String OFFSET = "offset";
    String ALL = "all";
    String BLOCK = "block";
    String ORDER_FIELDS = "order_fields";
    String DIRECTIONS = "directions";
    String GROUP_FIELDS = "group_fields";
    String AGG_FUNCS = "agg_funcs";
    String SCOPE = "scope";
    String TUPLES = "tuples";
    String JOIN_TYPE = "join_type";
    String SEMI_JOIN_TYPE = "semi_join_type";
    String BUILD_NODE = "build_node";
    String PROBE_NODE = "probe_node";
    String JOIN_SYSTEM_FIELD_NUM = "system_field_num";
    String REQUIRED_COLUMNS = "required_columns";
    String CORRELATE_ID = "correlate_id";
    String HINTS_ATTR = "hints";
    String PARALLEL_NUM = "parallel_num";
    String PARALLEL_INDEX = "parallel_index";
    String RESERVE_MAX_COUNT = "reserve_max_count";

    // attr name: nest table
    String USE_NEST_TABLE = "use_nest_table";
    String NEST_FIELD_NAMES = "nest_field_names";
    String NEST_FIELD_TYPES = "nest_field_types";
    String NEST_FIELD_COUNTS = "nest_field_counts";
    String WITH_ORDINALITY = "with_ordinality";
    String UNCOLLECT_ATTRS = "uncollect_attrs";

    // attr name: agg function
    String DISTINCT = "distinct";
    String APPROXIMATE = "approximate";
    String FILTER_ARG = "filter_arg";

    // attr name: match
    String MATCH_MEASURES = "measures";
    String MATCH_PATTERN = "pattern";
    String MATCH_STRICT_START = "strict_start";
    String MATCH_STRICT_END = "strict_end";
    String MATCH_ALL_ROWS = "all_rows";
    String MATCH_AFTER = "after";
    String MATCH_PATTERN_DEFINITIONS = "pattern_definitions";
    String MATCH_AGGREGATE_CALLS = "aggregate_calls";
    String MATCH_AGGREGATE_CALLS_PRE_VAR = "aggregate_calls_pre_var";
    String MATCH_SUBSETS = "subsets";
    String MATCH_PARTITION_KEYS = "partition_keys";
    String MATCH_INTERVAL = "interval";

    // attr key
    String ATTRS = "attrs";
    String NEST_TABLE_ATTRS = "nest_table_attrs";
    String PUSH_DOWN_OPS = "push_down_ops";

    // plan key
    String REL_PLAN_KEY = "rel_plan";
    String REL_PLAN_VERSION_KEY = "rel_plan_version";
    String EXEC_PARAMS_KEY = "exec_params";
    String OPTIMIZE_INFOS = "optimize_infos";
    String OPTIMIZE_INFO = "optimize_info";
    String PLAN_META = "plan_meta";

    // value
    String EMPTY = "";
    Boolean TRUE = true;
    Boolean FALSE = false;
    String OTHER = "OTHER";

    // other
    String FIELD_IDENTIFY = "$";
    String TABLE_IDENTIFY = "@";
    String TABLE_SEPARATOR = "#";
    String PATH_SEPARATOR = ".";
    String CAST = "CAST";
    String CAST_TYPE = "cast_type";
    String ORDINAL = "ordinal";
    String LEFT_PARENTHESIS = "(";
    String RIGHT_PARENTHESIS = ")";
    String LEFT_BRACKET = "[";
    String RIGHT_BRACKET = "]";
    String LEFT_BIG_PARENTHESIS = "{";
    String RIGHT_BIG_PARENTHESIS = "}";
    String NEW_LINE = "\n";
    String SPACE = " ";
    String COMMA = ",";
    String SEMICOLON = ";";
    String EQUAL = "=";
    String GREATER_THAN_OR_EQUAL = ">=";
    String LESS_THAN_OR_EQUAL = "<=";
    String COLON = ":";
    String DOT = ".";
    String DOT_REGEX = "\\.";
    String VERTICAL_LINE = "|";
    String VERTICAL_LINE_REGEX = "\\|";
    String IN = "IN";
    String AND = "AND";
    String OR = "OR";
    String CONTAIN = "CONTAIN";
    String CONTAIN2 = "_CONTAIN";
    String HA_IN = "HA_IN";
    String TEMPLATE_TABLE_SEPARATION = "@";
    String DEFAULT_FIELD_NAME = "f";

    String DYNAMIC_PARAMS_PREFIX = "[dynamic_params[[";
    String DYNAMIC_PARAMS_SUFFIX = "]]dynamic_params]";
    String DYNAMIC_PARAMS_SEPARATOR = "#";

    String REPLACE_PARAMS_PREFIX = "[replace_params[[";
    String REPLACE_PARAMS_SUFFIX = "]]replace_params]";
    String REPLACE_PARAMS_SEPARATOR = "#";
    String REPLACE_PARAMS_NUM = "replace_params_num";
    String PHYSICAL_TYPE = "physical_type";
    String MATCH_TYPE = "match_type";
    String REPLACE_KEY = "replace_key";
    String REPLACE_TYPE = "replace_type";

    // node's name
    String IQUAN_JOIN = "IquanJoinOp";
    String NEST_LOOP_JOIN = "NestedLoopJoinOp";
    String LOOKUP_JOIN = "LookupJoinOp";
    String HASH_JOIN = "HashJoinOp";
    String MULTI_JOIN = "MultiJoinOp";
    String LEFT_MULTI_JOIN_OP = "LeftMultiJoinOp";

    // hint
    String LOCAL_PARALLEL_HINT = "LOCAL_PARALLEL";
    String NO_INDEX_HINT = "NO_INDEX";
    String HASH_JOIN_HINT = "HASH_JOIN";
    String LOOKUP_JOIN_HINT = "LOOKUP_JOIN";
    String NORMAL_AGG_HINT = "NORMAL_AGG";
    String SCAN_ATTR_HINT = "SCAN_ATTR";
    String JOIN_ATTR_HINT = "JOIN_ATTR";
    String AGG_ATTR_HINT = "AGG_ATTR";

    String HINT_FIELDS_KEY = "fields";
    String HINT_TABLE_NAME_KEY = "tableName";
    String HINT_DISTRIBUTION_CHECK_KEY = "distributionCheck";
    String HINT_ASSIGNED_PARTITION_IDS_KEY = "partitionIds";
    String HINT_ASSIGNED_HASH_VALUES_KEY = "hashValues";
    String HINT_PROP_SCOPE_KEY = "propScope";
    String HINT_PROP_SCOPE_ALL_VALUE = "all";
    String HINT_PARALLEL_NUM_KEY = "parallelNum";

    String TRUE_VALUE = "true";
    String FALSE_VALUE = "false";

    // tvf params
    String NORMAL_SCOPE = "normal_scope";
    String ENABLE_SHUFFLE = "enable_shuffle";

    // udxf params
    String RETURN_CONST = "return_const";
    String INPUT_IDX = "input_idx";
    String MODIFY_INPUTS = "modify_inputs";

    // other
    String PARTITION_PRUNING = "partition_pruning";
    String JOIN_DECISION_MAP_FILE = "opt/equi_join_decision_map.json";
    String PROPERTIES = "properties";

    // source
    String SOURCE_TYPE = "source_type";

    // location
    String TABLE_GROUP_NAME= "table_group_name";

    Set<String> SupportSortFunctionNames = ImmutableSet.of(
            "sortTvf", "_sortTvf"
    );

    //agg index
    String AGG_INDEX_NAME = "aggregation_index_name";
    String AGG_KEYS = "aggregation_keys";
    String AGG_TYPE = "aggregation_type";
    String AGG_VALUE_FIELD= "aggregation_value_field";
    String AGG_DISTINCT = "aggregation_distinct";
    String AGG_RANGE_MAP = "aggregation_range_map";
}
