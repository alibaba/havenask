package com.taobao.search.iquan.core.api.common;

public enum IquanErrorCode {

    IQUAN_SUCCESS(0, null, "success"),
    IQUAN_FAIL(-1, null, "fail"),

    // catalog table
    IQUAN_EC_CATALOG_TABLE(100, null, "error occur in catalog table"),
    IQUAN_EC_CATALOG_TABLE_UPDATE_FAIL(101, IQUAN_EC_CATALOG_TABLE, "update table fail"),
    IQUAN_EC_CATALOG_TABLE_MODEL_INVALID(102, IQUAN_EC_CATALOG_TABLE, "table model is not valid"),
    IQUAN_EC_CATALOG_TABLE_CREATE_FAIL(103, IQUAN_EC_CATALOG_TABLE, "create table fail"),
    IQUAN_EC_CATALOG_TABLE_DROP_FAIL(104, IQUAN_EC_CATALOG_TABLE, "drop table fail"),
    IQUAN_EC_CATALOG_TABLE_PATH_INVALID(105, IQUAN_EC_CATALOG_TABLE, "table path is not valid"),
    IQUAN_EC_CATALOG_TABLE_QUALIFIER_INVALID(106, IQUAN_EC_CATALOG_TABLE, "table qualifier is not valid"),
    IQUAN_EC_CATALOG_ADD_CATALOG_FAIL(107, IQUAN_EC_CATALOG_TABLE, "add catalog fail"),
    IQUAN_EC_CATALOG_COMPUTE_NODES_UPDATE_FAIL(108, IQUAN_EC_CATALOG_TABLE, "update compute nodes fail"),

    // catalog function
    IQUAN_EC_CATALOG_FUNCTION(200, null, "error occur in catalog function"),
    IQUAN_EC_CATALOG_FUNCTION_UPDATE_FAIL(201, IQUAN_EC_CATALOG_FUNCTION, "update function fail"),
    IQUAN_EC_CATALOG_FUNCTION_MODEL_INVALID(202, IQUAN_EC_CATALOG_FUNCTION, "function model is not valid"),
    IQUAN_EC_CATALOG_FUNCTION_CREATE_FAIL(203, IQUAN_EC_CATALOG_FUNCTION, "create function fail"),
    IQUAN_EC_CATALOG_FUNCTION_DROP_FAIL(204, IQUAN_EC_CATALOG_FUNCTION, "drop function fail"),
    IQUAN_EC_CATALOG_FUNCTION_PATH_INVALID(205, IQUAN_EC_CATALOG_FUNCTION, "function path is not valid"),
    IQUAN_EC_CATALOG_FUNCTION_QUALIFIER_INVALID(206, IQUAN_EC_CATALOG_FUNCTION, "function qualifier is not valid"),

    // catalog error
    IQUAN_EC_CATALOG(300, null, "error occur in catalog"),
    IQUAN_EC_CATALOG_CATALOG_NOT_EXIST(301, IQUAN_EC_CATALOG, "catalog is not exist"),
    IQUAN_EC_CATALOG_DATABASE_NOT_EXIST(302, IQUAN_EC_CATALOG, "database is not exist"),
    IQUAN_EC_CATALOG_INTERNAL_ERROR(303, IQUAN_EC_CATALOG, "internal error for catalog"),

    // distribution error
    IQUAN_EC_DISTRIBUTION(400, null, "error occur in distribution"),
    IQUAN_EC_DISTRIBUTION_TYPE_INVALID(401, IQUAN_EC_DISTRIBUTION, "distribution type is not valid"),
    IQUAN_EC_DISTRIBUTION_RELNODE_INVALID(402, IQUAN_EC_DISTRIBUTION, "relnode type is not iquan"),
    IQUAN_EC_DISTRIBUTION_EMPTY_COMPUTE_NODES(403, IQUAN_EC_DISTRIBUTION, "compute nodes is empty"),
    IQUAN_EC_DISTRIBUTION_REORDER_HASH_FIELDS_FAIL(404, IQUAN_EC_DISTRIBUTION, "reorder hash fields fail"),
    IQUAN_EC_DISTRIBUTION_NO_SINGLE_COMPUTE_NODE(405, IQUAN_EC_DISTRIBUTION, "has no single compute node"),

    // mapper
    IQUAN_EC_MAPPER(1000, null, "error occur in mapper"),
    //mapper.table
    IQUAN_EC_MAPPER_TABLE_MODEL_INVALID(1001, IQUAN_EC_MAPPER, "table model is not valid"),
    IQUAN_EC_MAPPER_TABLE_MODEL_PATH_INVALID(1002, IQUAN_EC_MAPPER, "table path is not valid"),
    IQUAN_EC_MAPPER_TABLE_STATUS_INVALID(1003, IQUAN_EC_MAPPER, "table status is not valid"),
    IQUAN_EC_MAPPER_TABLE_EMPTY_IDS(1004, IQUAN_EC_MAPPER, "ids is empty"),
    //mapper.function
    IQUAN_EC_MAPPER_FUNCTION_MODEL_INVALID(1005, IQUAN_EC_MAPPER, "function model is not valid"),
    IQUAN_EC_MAPPER_FUNCTION_MODEL_PATH_INVALID(1006, IQUAN_EC_MAPPER, "function path is not valid"),
    IQUAN_EC_MAPPER_FUNCTION_STATUS_INVALID(1007, IQUAN_EC_MAPPER, "function status is not valid"),
    IQUAN_EC_MAPPER_FUNCTION_EMPTY_IDS(1008, IQUAN_EC_MAPPER, "ids is empty"),

    // sql
    // sql.agg
    IQUAN_EC_SQL_AGG(2000, null, "error occur in agg op"),
    IQUAN_EC_SQL_AGG_INDICATOR(2001, IQUAN_EC_SQL_AGG, "group sets is not support"),
    IQUAN_EC_SQL_AGG_UNSUPPORT_AGG_FUNCTION(2002, IQUAN_EC_SQL_AGG, "agg function is not support"),
    IQUAN_EC_SQL_AGG_GROUPING_FUNC_MAX_ARGS(2003,IQUAN_EC_SQL_AGG,"too many parameters in GROUPING func"),

    // sql.values
    IQUAN_EC_SQL_VALUES(2100, null, "error occur in values op"),
    IQUAN_EC_SQL_VALUES_NOT_EMPTY_VALUES(2101, IQUAN_EC_SQL_VALUES, "values is not empty"),

    // sql.uncollect
    IQUAN_EC_SQL_UNCOLLECT(2200, null, "error occur in uncollect"),
    IQUAN_EC_SQL_UNCOLLECT_NOT_EMPTY_ROWTYPE(2201, IQUAN_EC_SQL_UNCOLLECT, "rowtype is not empty"),
    IQUAN_EC_SQL_UNCOLLECT_EMPTY_ROWTYPE(2202, IQUAN_EC_SQL_UNCOLLECT, "rowtype is empty"),
    IQUAN_EC_SQL_UNCOLLECT_EMPTY_REXPROGRAM(2203, IQUAN_EC_SQL_UNCOLLECT, "rexprogram is empty"),
    IQUAN_EC_SQL_UNCOLLECT_UNSUPPORT_TYPE(2204, IQUAN_EC_SQL_UNCOLLECT, "unnest type is not support"),

    // sql.calc
    IQUAN_EC_SQL_CALC(2300, null, "error occur in calc op"),
    IQUAN_EC_SQL_CALC_INVALID_REXPROGRAM(2301, IQUAN_EC_SQL_CALC, "rexprogram is not valid"),

    // sql.order
    IQUAN_EC_SQL_ORDER(2400, null, "error occur in order op"),
    IQUAN_EC_SQL_ORDER_NOT_LIMIT(2401, IQUAN_EC_SQL_ORDER, "order by must used with limit"),
    IQUAN_EC_SQL_ORDER_UNSUPPORT_DYNAMIC_PARAM(2402, IQUAN_EC_SQL_ORDER, "order by not support dynamic param"),

    // sql.sink
    IQUAN_EC_SQL_SINK(2500, null, "error occur in sink op"),
    IQUAN_EC_SQL_SINK_UNSUPPORT_TYPE(2501, null, "sink type is not support"),

    //sql.union
    IQUAN_EC_SQL_UNION(2600, null, "error occur in union op"),
    IQUAN_EC_SQL_UNION_MIXED_TYPE(2601, IQUAN_EC_SQL_UNION, "union all and union mix is not support"),
    IQUAN_EC_SQL_UNION_ALL_ONLY(2602, IQUAN_EC_SQL_UNION, "union not support"),
    IQUAN_EC_SQL_UNION_INIT_NULL(2603, IQUAN_EC_SQL_UNION, "IquanUnionOp locationList init error"),
    IQUAN_EC_SQL_UNION_INPUT_ERROR(2604, IQUAN_EC_SQL_UNION, "input relNode location is null"),

    //sql.table.scan
    IQUAN_EC_SQL_SCAN(2700, null, "error occur in table scan op"),
    IQUAN_EC_SQL_SCAN_EMPTY_REXPROGRAM(2701, IQUAN_EC_SQL_SCAN, "rexprogram is empty"),

    //sql.correlate
    IQUAN_EC_SQL_CORRELATE(2800, null, "error occur in correlate op"),
    IQUAN_EC_SQL_CORRELATE_UNSUPPORT_JOIN_TYPE(2801, IQUAN_EC_SQL_CORRELATE, "join type is not support"),
    IQUAN_EC_SQL_CORRELATE_EMPTY_REXPROGRAM(2802, IQUAN_EC_SQL_CORRELATE, "rexProgram is not empty"),

    //sql.table.function.scan
    IQUAN_EC_SQL_TABLE_FUNCTION_SCAN(2900, null, "error occur in table function scan op"),
    IQUAN_EC_SQL_TABLE_FUNCTION_SCAN_UNSUPPORT_NESTED_TVF(2901, IQUAN_EC_SQL_TABLE_FUNCTION_SCAN, "nested tvf function is not support"),

    //sql.join
    IQUAN_EC_SQL_JOIN(3000, null, "error occur in join op"),
    IQUAN_EC_SQL_JOIN_UNSUPPORT_JOIN_TYPE(3001, IQUAN_EC_SQL_JOIN, "join type is not support"),
    IQUAN_EC_SQL_JOIN_UNSUPPORT_TABLE_TYPE(3002, IQUAN_EC_SQL_JOIN, "table type is not support"),
    IQUAN_EC_SQL_JOIN_INVALID_CONDITION(3003, IQUAN_EC_SQL_JOIN, "join condition is not valid"),
    IQUAN_EC_SQL_LOOKUP_JOIN_INVALID(3004, IQUAN_EC_SQL_JOIN, "lookup join is not valid"),
    IQUAN_EC_SQL_JOIN_INVALID(3005, IQUAN_EC_SQL_JOIN, "join is not valid"),
    IQUAN_EC_SQL_LEFT_MULTI_JOIN_INVALID(3006, IQUAN_EC_SQL_JOIN, "left multi join is not valid"),

    // sql.workflow
    IQUAN_EC_SQL_WORKFLOW(5000, null, "error occur in workflow"),
    IQUAN_EC_SQL_WORKFLOW_CONVERT_FAIL(5001, IQUAN_EC_SQL_WORKFLOW, "relnode convert fail"),
    IQUAN_EC_SQL_WORKFLOW_EMPTY_RELNODE(5002, IQUAN_EC_SQL_WORKFLOW, "relnode is empty"),
    IQUAN_EC_SQL_WORKFLOW_EMPTY_SQLNODE(5003, IQUAN_EC_SQL_WORKFLOW, "sqlnode is empty"),
    IQUAN_EC_SQL_WORKFLOW_UNSUPPORT_SQL_TYPE(5004, IQUAN_EC_SQL_WORKFLOW, "sql type is not in (SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY)"),
    IQUAN_EC_SQL_WORKFLOW_EMPTY_RAW_RELNODE(5005, IQUAN_EC_SQL_WORKFLOW, "raw relnode is empty"),
    IQUAN_EC_SQL_WORKFLOW_EMPTY_OPT_RELNODE(5006, IQUAN_EC_SQL_WORKFLOW, "optimized relnode is empty"),
    IQUAN_EC_SQL_WORKFLOW_INVALID_CATALOG_INFO(5007, IQUAN_EC_SQL_WORKFLOW, "invalid catalog info"),
    IQUAN_EC_SQL_WORKFLOW_CAN_NOT_GET_VALID_DB_NAME(5008, IQUAN_EC_SQL_WORKFLOW, "optional db is invalid"),
    IQUAN_EC_SQL_WORKFLOW_INVALID_EXEC_PHASE(5009, IQUAN_EC_SQL_WORKFLOW, "exec phase is invalid"),
    IQUAN_EC_SQL_WORKFLOW_INVALID_INPUT_PARAMS(5010, IQUAN_EC_SQL_WORKFLOW, "input params is invalid"),

    // plan format
    IQUAN_EC_PLAN_FORMAT(6000, null, "error occur in plan format"),
    IQUAN_EC_PLAN_UNSUPPORT_FORMAT_TYPE(6001, IQUAN_EC_PLAN_FORMAT, "type is not support"),
    IQUAN_EC_PLAN_UNSUPPORT_FORMAT_VERSION(6002, IQUAN_EC_PLAN_FORMAT, "version is not support"),
    IQUAN_EC_PLAN_UNSUPPORT_RELDATA_TYPE(6003, IQUAN_EC_PLAN_FORMAT, "RelData type is not support"),
    IQUAN_EC_PLAN_RESULT_INVALID(6004, IQUAN_EC_PLAN_FORMAT, "plan result is not valid"),
    IQUAN_EC_PLAN_PLAN_OP_INVALID(6005, IQUAN_EC_PLAN_FORMAT, "plan op is not valid"),
    IQUAN_EC_PLAN_UNSUPPORT_VALUE_TYPE(6006, IQUAN_EC_PLAN_FORMAT, "value type is not support"),
    IQUAN_EC_PLAN_HASH_VALUES_MERGE_ERROR(6007, IQUAN_EC_PLAN_FORMAT, "table hash values can not be merged"),

    // config
    IQUAN_EC_CONF(7000, null, "error occur in config"),
    IQUAN_EC_CONF_UNSUPPORT_KEY(7001, IQUAN_EC_CONF, "key is not support"),

    // boot common
    IQUAN_EC_BOOT_COMMON(8000, null, "error occur in boot common"),
    IQUAN_EC_BOOT_COMMON_NOT_CONTAIN_KEY(8001, IQUAN_EC_BOOT_COMMON, "key is not contain"),
    IQUAN_EC_BOOT_COMMON_INVALID_KEY(8002, IQUAN_EC_BOOT_COMMON, "key is not valid"),

    // iquan client
    IQUAN_EC_CLIENT(9000, null, "error occur in client"),
    IQUAN_EC_CLIENT_INIT_FAIL(9001, IQUAN_EC_CLIENT, "init iquan client fail"),
    IQUAN_EC_CLIENT_UNSUPPORT_REQUEST_FORMAT_TYPE(9002, IQUAN_EC_CLIENT, "request format type is not support"),
    IQUAN_EC_CLIENT_REQUEST_ERROR(9003, IQUAN_EC_CLIENT, "request is not valid"),
    IQUAN_EC_CAN_NOT_GET_RESOURCE_ERROR(9004, IQUAN_EC_CLIENT, "failed to get resource"),
    IQUAN_EC_CAN_FAIL_TO_WARMUP_ERROR(9005, IQUAN_EC_CLIENT, "failed to get resource"),
    IQUAN_EC_INVALID_WARMUP_INFO_ERROR(9006, IQUAN_EC_CLIENT, "invalid warmup info"),
    IQUAN_EC_INVALID_TABLE_PATH(9007, IQUAN_EC_CLIENT, "table path is invalid"),
    IQUAN_EC_INVALID_FUNCTION_PATH(9008, IQUAN_EC_CLIENT, "function path is invalid"),
    IQUAN_EC_INVALID_CATALOG_PATH(9009, IQUAN_EC_CLIENT, "function path is invalid"),
    IQUAN_EC_INVALID_LAYER_TABLE_PATH(9010, IQUAN_EC_CLIENT, "layerTable path is invalid"),
    IQUAN_EC_LAYER_TABLE_PROPERTIES_BIZ(9011, IQUAN_EC_CLIENT, "layerTable properties biz invalid"),

    // dynamic params
    IQUAN_EC_DYNAMIC_PARAMS(10000, null, "error occur in dynamic params"),
    IQUAN_EC_DYNAMIC_PARAMS_INVALID_PARAMS(10001, IQUAN_EC_DYNAMIC_PARAMS, "input dynamic params is not valid"),
    IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_PARAM_TYPE(10002, IQUAN_EC_DYNAMIC_PARAMS, "dynamic param type is not support"),
    IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_PREPARE_LEVEL(10003, IQUAN_EC_DYNAMIC_PARAMS, "prepare level is not support"),
    IQUAN_EC_DYNAMIC_PARAMS_PREPARE_FAIL(10004, IQUAN_EC_DYNAMIC_PARAMS, "prepare fail"),
    IQUAN_EC_DYNAMIC_PARAMS_COPY_FAIL(10005, IQUAN_EC_DYNAMIC_PARAMS, "deep copy rel node fail"),
    IQUAN_EC_DYNAMIC_PARAMS_UNSUPPORT_NODE_TYPE(10005, IQUAN_EC_DYNAMIC_PARAMS, "rel node type is not support to copy"),
    IQUAN_EC_DYNAMIC_PARAMS_LACK_OF_INPUT(10006, IQUAN_EC_DYNAMIC_PARAMS, "lack of input dynamic params"),


    // hint
    IQUAN_EC_HINT(11000, null, "error in hint"),
    IQUAN_EC_HINT_NOT_SUPPORT(11001, IQUAN_EC_HINT, "unsupported this hint"),

    IQUAN_EC_TABLE_MODIFY(12000, null, "error in table modify"),
    IQUAN_EC_UNSUPPORTED_TABLE_MODIFY(12001, IQUAN_EC_TABLE_MODIFY, "unsupported table modify"),

    // optimizer level
    IQUAN_EC_OPTIMIZER_LEVEL(13000, null, "error in optimizer level"),
    IQUAN_EC_OPTIMIZER_UNSUPPORTED_LEVEL(13001, IQUAN_EC_OPTIMIZER_LEVEL, "unsupported optimizer level"),

    // internal
    IQUAN_EC_INTERNAL(90000, null, "internal error"),
    IQUAN_EC_INTERNAL_ERROR(90001, IQUAN_EC_INTERNAL, "internal error"),
    IQUAN_EC_UNSUPPORT_FORMAT_TYPE_ERROR(90002, IQUAN_EC_INTERNAL, "unsupported format type"),
    IQUAN_EC_CAN_NOT_REACH_HERE(90003, IQUAN_EC_INTERNAL, "can not reach here");


    private int errorCode;
    private String errorMessage;
    private IquanErrorCode groupErrorCode;

    IquanErrorCode(int errorCode, IquanErrorCode groupErrorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = "[" + name() + "] " + errorMessage;
        this.groupErrorCode = groupErrorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage(String message) {
        if (message == null || message.isEmpty()) {
            return errorMessage;
        }

        return errorMessage + " : " + message;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getGroupErrorCode() {
        if (groupErrorCode == null) {
            return this.errorCode;
        }
        return groupErrorCode.errorCode;
    }

    public String getGroupErrorMessage(String message) {
        if (groupErrorCode == null) {
            return "";
        }

        String errorMsg = groupErrorCode.getErrorMessage();
        if (message == null || message.isEmpty()) {
            return errorMsg;
        }

        return errorMsg + " : " + message;
    }

    public String getGroupErrorMessage() {
        if (groupErrorCode == null) {
            return "";
        }
        return groupErrorCode.getErrorMessage();
    }
}
