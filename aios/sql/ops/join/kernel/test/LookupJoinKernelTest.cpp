#include "sql/ops/join/LookupJoinKernel.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/ColumnTerm.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/TableQuery.h"
#include "ha3/common/Term.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/common/IndexInfo.h"
#include "sql/data/TableData.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace isearch::common;

namespace sql {

static const string DefaultBuildNode
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json";
static const string BuildNodeBatch
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":5, "hash_fields":["$id"]})json";
static const string BuildNodeNumberIndex
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json";
static const string BuildNodeNoPk
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json";
static const string BuildNodeNoPkNoString
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json";
static const string BuildNodeNumberIndexPartial
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json";
static const string BuildNodeNumberIndexPartialLargeBatch
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":10000, "hash_fields":["$aid"]})json";
static const string BuildNodeNumberIndexCondition
    = R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "field_meta": [{"field_name": "$aid", "field_type": "int64", "index_name": "aid", "index_type": "primarykey64"}, {"field_name": "$path", "field_type": "string", "index_name": "path", "index_type": "STRING"}, {"field_name": "$attr", "field_type": "int32", "index_name": "attr", "index_type": "NUMBER"}, {"field_name": "$group", "field_type": "string"}], "output_fields_internal":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "condition":{"op":"=", "params":["$aid", 1]},"batch_size":1, "hash_fields": ["$id"]})json";
static const string DefaultTableMeta
    = R"json({"field_meta": [{"field_type": "int32", "field_name": "$aid", "index_name": "aid", "index_type": "primarykey64"}, {"field_type": "STRING", "field_name": "$path", "index_name": "path", "index_type": "STRING"}]})json";
static const string TableMetaWithPk
    = R"json({"field_meta": [{"field_type": "int32", "field_name": "$aid", "index_name": "aid", "index_type": "primarykey64"},{"field_type": "int32", "field_name": "$attr", "index_name": "attr", "index_type": "primarykey64"}, {"field_type": "STRING", "field_name": "$path", "index_name": "path", "index_type": "STRING"}]})json";
static const string TableMetaWithNumber
    = R"json({"field_meta": [{"field_type": "int32", "field_name": "$aid", "index_name": "aid", "index_type": "primarykey64"},{"field_type": "int32", "field_name": "$attr", "index_name": "attr", "index_type": "NUMBER"}, {"field_type": "STRING", "field_name": "$path", "index_name": "path", "index_type": "STRING"}]})json";
static const string TableMetaNoPk
    = R"json({"field_meta": [{"field_type": "int32", "field_name": "$aid", "index_name": "aid", "index_type": "NUMBER"},{"field_type": "int32", "field_name": "$attr", "index_name": "attr", "index_type": "NUMBER"}, {"field_type": "STRING", "field_name": "$path", "index_name": "path", "index_type": "STRING"}]})json";
static const string TmpTableMeta
    = R"json({"field_meta": [{"field_type": "int32", "field_name": "$aid", "index_name": "aid", "index_type": "primarykey64"}, {"field_type": "int32", "field_name": "$attr", "index_name": "attr", "index_type": "NUMBER"}]})json";

class LookupJoinKernelTest : public OpTestBase {
public:
    LookupJoinKernelTest();
    ~LookupJoinKernelTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _attributeMap.clear();
    }

private:
    void initBaiscAttribute(bool leftIsBuild, string buildMode = DefaultBuildNode) {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = leftIsBuild;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        if (!buildMode.empty()) {
            _attributeMap["build_node"] = ParseJson(buildMode);
        }
    }

    void prepareAttribute() {
        initBaiscAttribute(false);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
    }

    void prepareAttributeLeftBuildIsTrue() {
        initBaiscAttribute(true);
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group","$fid", "$path0"])json");
    }

    void preparePartialIndexJoinLeftBuildIsTrue() {
        initBaiscAttribute(true);
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$path", "$group"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group","$fid", "$path0", "$group0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$path","$path0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]}]})json");
    }

    void preparePartialIndexSemiJoin() {
        initBaiscAttribute(true);
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$group"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$aid", "$path", "$group","$aid0","$group0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$aid","$aid0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]}]})json");
    }

    void prepareAttributeSemiJoin() {
        initBaiscAttribute(true, BuildNodeBatch);
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$aid", "$path", "$group","$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
    }

    void prepareAttributeSemiJoinRightIsBuild() {
        initBaiscAttribute(false, BuildNodeBatch);
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_table_meta"] = ParseJson(DefaultTableMeta);
    }

    void prepareAttributeAntiJoinRightIsBuild() {
        initBaiscAttribute(false, BuildNodeBatch);
        _attributeMap["join_type"] = string("ANTI");
        _attributeMap["semi_join_type"] = string("ANTI");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_table_meta"] = ParseJson(DefaultTableMeta);
    }

    void prepareAttributeMultiFiledJoin() {
        initBaiscAttribute(false, BuildNodeNumberIndex);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaWithPk);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
    }

    void prepareAttributeMultiFiledSemiJoin() {
        initBaiscAttribute(false, BuildNodeNumberIndex);
        _attributeMap["join_type"] = string("SEMI");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaWithNumber);
        _attributeMap["output_fields"] = ParseJson(R"json(["$fid", "$path","$attr"])json");
        _attributeMap["output_fields_internal"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
    }

    void prepareAttributeMultiFiledJoinHasQuery() {
        initBaiscAttribute(false, BuildNodeNumberIndexCondition);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaWithNumber);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
    }

    void prepareAttributeMultiFiledJoinHasFilter() {
        initBaiscAttribute(false, "");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaWithPk);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields_internal":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "condition":{"op":">", "params":["$aid", 1]},"batch_size":1, "hash_fields": ["$id"]})json");
    }

    void prepareAttributePKJoinOptimize() {
        initBaiscAttribute(false);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$id"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        _attributeMap["right_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"] = ParseJson(R"json(["$fid", "$id", "$aid", "$path"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$aid"]})json");
    }

    void prepareAttributePKJoinOptimizeRightIsBuild() {
        initBaiscAttribute(true);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$id"])json");
        _attributeMap["left_table_meta"] = ParseJson(DefaultTableMeta);
        _attributeMap["output_fields"] = ParseJson(R"json(["$aid", "$path", "$fid", "$id"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$aid"]})json");
    }

    void prepareAttributeNumberJoinOptimizeRightIsBuild() {
        initBaiscAttribute(true, BuildNodeNumberIndexPartial);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_table_meta"] = ParseJson(TmpTableMeta);

        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$attr", "$fattr"]})json");
    }

    void prepareAttributeNumberJoinLargeBatch() {
        initBaiscAttribute(true, BuildNodeNumberIndexPartialLargeBatch);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_table_meta"] = ParseJson(TmpTableMeta);

        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$attr", "$fattr"]})json");
    }

    void prepareAttributeJoinCache() {
        initBaiscAttribute(true, BuildNodeNumberIndexPartial);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_table_meta"] = ParseJson(TmpTableMeta);

        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]},{"op":"=","params":["$aid", "$fid"]}]})json");
    }

    void prepareAttributeJoinRowDeduplicate() {
        initBaiscAttribute(true, BuildNodeNoPkNoString);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_table_meta"] = ParseJson(TmpTableMeta);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]},{"op":"=","params":["$aid", "$fid"]}]})json");
    }

    void prepareAttributeJoinMultiValueRowDeduplicate() {
        initBaiscAttribute(false, BuildNodeNoPk);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaNoPk);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]},{"op":"=","params":["$fid", "$aid"]}]})json");
    }

    void prepareAttributeJoinRowDeduplicateForSingleColumn() {
        initBaiscAttribute(true, BuildNodeNoPkNoString);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_table_meta"] = ParseJson(TmpTableMeta);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]}]})json");
    }

    void prepareAttributeJoinMultiValueRowDeduplicateForSingleColumn() {
        initBaiscAttribute(false, BuildNodeNoPk);
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_table_meta"] = ParseJson(TableMetaNoPk);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op": "AND", "params":[{"op":"=","params":["$path", "$path0"]}]})json");
    }

    void prepareInvertedTableData(string &tableName,
                                  string &fields,
                                  string &indexes,
                                  string &attributes,
                                  string &summarys,
                                  string &truncateProfileStr,
                                  string &docs,
                                  int64_t &ttl) override {
        _tableName = "acl";
        tableName = _tableName;
        fields = "aid:int64;path:string;group:string;attr:int32";
        indexes = "aid:primarykey64:aid;path:string:path;attr:number:attr";
        attributes = "aid;path;group;attr";
        summarys = "aid;path;group;attr";
        truncateProfileStr = "";
        docs = "cmd=add,aid=0,path=/a,attr=1,group=groupA;"
               "cmd=add,aid=1,path=/a/b,attr=1,group=groupA;"
               "cmd=add,aid=2,path=/a/b,attr=2,group=groupB;"
               "cmd=add,aid=3,path=/a/b/c,attr=2,group=groupB;"
               "cmd=add,aid=4,path=/a/d/c,attr=3,group=groupA;"
               "cmd=add,aid=5,path=/a/c/c,attr=1,group=groupA;"
               "cmd=add,aid=6,path=/a/c,attr=2,group=groupB;"
               "cmd=add,aid=-1,path=/a/b/c/d,attr=4,group=groupC;";
        ttl = numeric_limits<int64_t>::max();
    }
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

    TablePtr prepareTable() {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 1);
        IF_FAILURE_RET_NULL(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {1}));
        IF_FAILURE_RET_NULL(
            _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"abc"}));
        vector<string> subPaths = {"/a", "/a/b/c", "/a/b"};
        char *buf = MultiValueCreator::createMultiStringBuffer(subPaths, _poolPtr.get());
        MultiString multiPath(buf);
        IF_FAILURE_RET_NULL(_matchDocUtil.extendMatchDocAllocator<MultiString>(
            _allocator, leftDocs, "pathM", {multiPath}));
        vector<int32_t> ids = {1, 2};
        buf = MultiValueCreator::createMultiValueBuffer(ids.data(), 2, _poolPtr.get());
        MultiInt32 multiIds(buf);
        IF_FAILURE_RET_NULL(_matchDocUtil.extendMatchDocAllocator<MultiInt32>(
            _allocator, leftDocs, "ids", {multiIds}));
        IF_FAILURE_RET_NULL(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "docid", {2}));
        TablePtr table(new Table(leftDocs, _allocator));
        return table;
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount) << TableUtil::toString(outputTable);
        auto column = outputTable->getColumn(field);
        auto name = column->getColumnSchema()->getName();
        ASSERT_TRUE(column != NULL);
        for (size_t i = 0; i < rowCount; i++) {
            ASSERT_EQ(expect[i], column->toString(i));
        }
    }

    void getOutputTable(KernelTesterPtr testerPtr, TablePtr &outputTable, bool &eof) {
        DataPtr odata;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        outputTable = getTable(odata);
        ASSERT_TRUE(outputTable != NULL);
    }

    void semiBuildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        _testerPtr = builder.build();
        ASSERT_TRUE(_testerPtr.get());
        ASSERT_FALSE(_testerPtr->hasError());
    }

    void prepareLeftTable(const vector<uint32_t> &fid, const vector<string> &path, bool eof) {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, fid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", fid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", path));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(_testerPtr->setInput("input0", leftTable, eof));
    }

    void checkIndexSemiOutput(const vector<int64_t> &aid,
                              const vector<string> &path,
                              const vector<string> &group) {
        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*_testerPtr, outputTable));

        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", aid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "path", path));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "group", group));
    }

    void checkInnerOutput(const vector<uint32_t> &fid,
                          const vector<string> &path,
                          const vector<int64_t> &aid,
                          const vector<string> &path0,
                          const vector<string> &group,
                          bool isEof,
                          bool hasOutput = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        if (hasOutput == false) {
            return;
        }
        ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
        ASSERT_EQ(isEof, eof);
        ASSERT_EQ(aid.size(), outputTable->getRowCount()) << TableUtil::toString(outputTable);
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", fid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "path", path));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", aid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "path", path0));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "group", group));
    }
    void checkSemiOutput(const vector<uint32_t> &fid, const vector<string> &path) {
        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*_testerPtr, outputTable));

        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", fid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "path", path));
    }

    void checkNoOutput() {
        if (_testerPtr->compute()) {
            ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
            TablePtr outputTable;
            bool eof = false;
            ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
        }
    }

    void checkOutputEof(bool isEof = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
        DataPtr odata;
        bool eof;
        ASSERT_TRUE(_testerPtr->getOutput("output0", odata, eof));
        ASSERT_EQ(eof, isEof);
    }

    void createMultiStringDocs(vector<MatchDoc> &leftDocs,
                               const vector<string> &subPaths = {},
                               const vector<string> &subPaths1 = {},
                               const vector<string> &subPaths2 = {},
                               const vector<uint32_t> &values = {0, 1, 2}) {
        leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        char *buf = MultiValueCreator::createMultiStringBuffer(subPaths, _poolPtr.get());
        MultiString multiPath(buf);
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, _poolPtr.get());
        MultiString multiPath1(buf);
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, _poolPtr.get());
        MultiString multiPath2(buf);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", values));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiString>(
            _allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, leftDocs, "attr", {1, 2, 1}));
    }
    void checkTableInput(KernelTesterPtr testerPtr,
                         const vector<string> &subPaths = {},
                         const vector<string> &subPaths1 = {},
                         const vector<string> &subPaths2 = {}) {
        vector<MatchDoc> leftDocs;
        createMultiStringDocs(leftDocs, subPaths, subPaths1, subPaths2);
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    void checkTableInputPath(KernelTesterPtr testerPtr) {
        vector<string> subPaths = {"/a", "/a/b/c", "/a/b"};
        vector<string> subPaths1 = {"/a/b/c"};
        vector<string> subPaths2 = {};
        checkTableInput(testerPtr, subPaths, subPaths1, subPaths2);
    }

    void testSingleValJoinRightFunc(const string &hint);
    void testSingleValJoinRightInputIsEmptyFunc(const string &hint);
    void testSingleValJoinRightScanIsEmptyFunc(const string &hint);
    void testSingleValJoinLeftFunc(const string &hint);
    void testPartialIndexJoinLeftFunc(const string &hint);
    void testSingleValJoinLeftWithBatchLimit1Func(const string &hint);
    void testSingleValJoinLeftWithLookupBatchLimit1Func(const string &hint);
    void testSingleValJoinLeftWithLimit1Func(const string &hint);
    void testSingleValJoinLeftWithThresholdHintsFunc(const string &hint);
    void testCacheDisableHintsFunc(const string &hint);
    void testCacheDisableHints2Func(const string &hint);
    void testSingleValueRowDeduplicateHintsFunc(const string &hint);
    void testMulitiValueRowDeduplicateHintsFunc(const string &hint);
    void testSingleValueRowDeduplicateHintsForSingleColumnFunc(const string &hint);
    void testMulitiValueRowDeduplicateHintsForSingleColumnFunc(const string &hint);
    void testMultiValueJoinFunc(const string &hint);
    void testMultiFieldsJoinFunc(const string &hint);
    void testEmptyMultiFieldsJoinFunc(const string &hint);
    void testMultiFieldsJoinWithQueryFunc(const string &hint);
    void testMultiFieldsJoinWithFilterFunc(const string &hint);
    void testMultiFieldsSemiJoinFunc(const string &hint);
    void testEmptyMultiFieldsSemiJoinFunc(const string &hint);
    void testMultiFieldsAntiJoinFunc(const string &hint);
    void testEmptyMultiFieldsAntiJoinFunc(const string &hint);
    void testPartialIndexJoinLeftSemiJoinFunc(const string &hint);
    void testNotEqualJoinFunc(const string &hint);
    void testLeftJoinFunc(const string &hint);
    void testLeftJoinSingleEmptyFunc(const string &hint);
    void testLeftJoinAllEmptyFunc(const string &hint);
    void testLeftJoinBatchInputFunc(const string &hint);
    void testLeftJoinBatchOutputFunc(const string &hint);
    void testPKJoinFunc(const string &hint);
    void testPKJoinRightIsBuildFunc(const string &hint);
    void testNumberJoinRightIsBuildFunc(const string &hint);

public:
    MatchDocAllocatorPtr _allocator;

private:
    KernelTesterPtr _testerPtr;
};

LookupJoinKernelTest::LookupJoinKernelTest() {}

LookupJoinKernelTest::~LookupJoinKernelTest() {}

void LookupJoinKernelTest::testSingleValJoinRightFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinRight) {
    string case0 {};
    string case1 = R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json";
    string case2 = R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json";

    testSingleValJoinRightFunc(case0);
    testSingleValJoinRightFunc(case1);
    testSingleValJoinRightFunc(case2);
}

void LookupJoinKernelTest::testSingleValJoinRightInputIsEmptyFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    _allocator.reset(new MatchDocAllocator(_poolPtr.get()));
    _allocator->declare<uint32_t>("fid", SL_ATTRIBUTE);
    _allocator->declare<MultiChar>("path", SL_ATTRIBUTE);
    auto leftTable = createTable(_allocator, {});
    ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    TablePtr leftInputTable = getTable(leftTable);
    ASSERT_TRUE(leftInputTable);
    checkDependentTable(leftInputTable, outputTable);
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_EQ(0, outputTable->getRowCount());
    checkOutput({}, odata, "fid");
    checkOutput({}, odata, "path");
    checkOutput({}, odata, "path0");
    checkOutput({}, odata, "group");
    checkOutput({}, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinRightInputIsEmpty) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValJoinRightInputIsEmptyFunc(hint);
    }
}

void LookupJoinKernelTest::testSingleValJoinRightScanIsEmptyFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a/cc", "/a/b/cc"}));
    auto leftTable = createTable(_allocator, leftDocs);
    ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_EQ(0, outputTable->getRowCount());
    TablePtr leftInputTable = getTable(leftTable);
    ASSERT_TRUE(leftInputTable);
    checkDependentTable(leftInputTable, outputTable);

    checkOutput({}, odata, "fid");
    checkOutput({}, odata, "path");
    checkOutput({}, odata, "path0");
    checkOutput({}, odata, "group");
    checkOutput({}, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinRightScanIsEmpty) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValJoinRightScanIsEmptyFunc(hint);
    }
}

void LookupJoinKernelTest::testSingleValJoinLeftFunc(const string &hint) {
    prepareAttributeLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
    auto rightTable = createTable(_allocator, docs);
    ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeft) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValJoinLeftFunc(hint);
    }
}

void LookupJoinKernelTest::testPartialIndexJoinLeftFunc(const string &hint) {
    preparePartialIndexJoinLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, docs, "group", {"group2A", "groupB"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expAid = {"2"};
    checkOutput(expAid, odata, "aid");
    vector<string> expPath = {"/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupB"};
    checkOutput(expGroup, odata, "group");
    checkOutput(expGroup, odata, "group0");
}

TEST_F(LookupJoinKernelTest, testPartialIndexJoinLeft) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"group"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path,group"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPartialIndexJoinLeftFunc(hint);
    }
}

void LookupJoinKernelTest::testSingleValJoinLeftWithBatchLimit1Func(const string &hint) {
    prepareAttributeLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
    auto rightTable = createTable(_allocator, docs);
    ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));

    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));

    {
        vector<uint32_t> expFid = {0, 1, 1};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "fid", expFid));
        vector<string> expPath = {"/a", "/a/b", "/a/b"};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", expPath));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", expPath));
        vector<string> expGroup = {"groupA", "groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", expGroup));
    }
    TablePtr inputTable = getTable(rightTable);
    ASSERT_TRUE(inputTable);
    checkDependentTable(inputTable, outputTable);
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithBatchLimit1) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"group"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path,group"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testSingleValJoinLeftWithBatchLimit1Func(hint));
    }
}

void LookupJoinKernelTest::testSingleValJoinLeftWithLookupBatchLimit1Func(const string &hint) {
    prepareAttributeLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }
    DataPtr odata;
    bool eof = false;
    while (1) {
        ASSERT_FALSE(testerPtr->hasError()) << testerPtr->getErrorMessage();
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        if (testerPtr->getOutput("output0", odata, eof)) {
            break;
        }
    }

    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithLookupBatchLimit1) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testSingleValJoinLeftWithLookupBatchLimit1Func(hint));
    }
}

void LookupJoinKernelTest::testSingleValJoinLeftWithLimit1Func(const string &hint) {
    prepareAttributeLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
    auto rightTable = createTable(_allocator, docs);
    ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));

    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
    {
        vector<uint32_t> expFid = {0, 1, 1};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "fid", expFid));
        vector<string> expPath = {"/a", "/a/b", "/a/b"};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", expPath));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", expPath));
        vector<string> expGroup = {"groupA", "groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", expGroup));
    }
    TablePtr inputTable = getTable(rightTable);
    ASSERT_TRUE(inputTable);
    checkDependentTable(inputTable, outputTable);
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithLimit1) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testSingleValJoinLeftWithLimit1Func(hint));
    }
}

void LookupJoinKernelTest::testSingleValJoinLeftWithThresholdHintsFunc(const string &hint) {
    prepareAttributeLeftBuildIsTrue();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    {
        vector<string> expFid = {"0"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithThresholdHints) {
    vector<string> hints {
        R"json({"JOIN_ATTR":{"lookupTurncateThreshold":"1","lookupBatchSize":"10"}})json",
        R"json({"JOIN_ATTR":{"truncateThreshold":"1"}})json",
        R"json({"JOIN_ATTR":{"truncateThreshold":"10", "lookupTurncateThreshold":"1"}})json",
        R"json({"JOIN_ATTR":{"lookupTurncateThreshold":"1","lookupBatchSize":"10","lookupDisableCacheFields":"path"}})json",
        R"json({"JOIN_ATTR":{"lookupTurncateThreshold":"1","lookupBatchSize":"10","lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValJoinLeftWithThresholdHintsFunc(hint);
    }
}

void LookupJoinKernelTest::testCacheDisableHintsFunc(const string &hint) {
    prepareAttributeJoinCache();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 2}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "aid");
    ASSERT_FALSE(colVec[0]->getEnableCache());
    ASSERT_EQ(colVec[1]->getIndexName(), "attr");
    ASSERT_FALSE(colVec[1]->getEnableCache());
}

TEST_F(LookupJoinKernelTest, testCacheDisableHints) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid,attr"}})json"};
    for (const auto &hint : hints) {
        testCacheDisableHintsFunc(hint);
    }
}

void LookupJoinKernelTest::testCacheDisableHints2Func(const string &hint) {
    prepareAttributeJoinCache();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 2}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "aid");
    ASSERT_FALSE(colVec[0]->getEnableCache());
    ASSERT_EQ(colVec[1]->getIndexName(), "attr");
    ASSERT_TRUE(colVec[1]->getEnableCache());
}

TEST_F(LookupJoinKernelTest, testCacheDisableHints2) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid"}})json"};
    for (const auto &hint : hints) {
        testCacheDisableHints2Func(hint);
    }
}

void LookupJoinKernelTest::testSingleValueRowDeduplicateHintsFunc(const string &hint) {
    prepareAttributeJoinRowDeduplicate();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 6);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
        _allocator, docs, "fid", {1, 22, 33, 1, 22, 1}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
        _allocator, docs, "fattr", {5, 2, 3, 3, 2, 5}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 6}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_EQ(colVec[1]->getIndexName(), "aid");
    vector<pair<int64_t, uint32_t>> results;
    auto castTermAttr = dynamic_cast<ColumnTermTyped<int64_t> *>(colVec[0]);
    auto castTermAid = dynamic_cast<ColumnTermTyped<uint32_t> *>(colVec[1]);
    ASSERT_TRUE(castTermAttr);
    ASSERT_TRUE(castTermAid);
    ASSERT_EQ(0, castTermAttr->getOffsets().size());
    ASSERT_EQ(0, castTermAid->getOffsets().size());
    ASSERT_EQ(4, castTermAttr->getValues().size());
    ASSERT_EQ(4, castTermAid->getValues().size());

    for (size_t i = 0; i < 4; ++i) {
        results.push_back(make_pair(castTermAttr->getValues()[i], castTermAid->getValues()[i]));
    }

    ASSERT_THAT(results,
                testing::UnorderedElementsAre(make_pair<int64_t, uint32_t>(3, 1),
                                              make_pair<int64_t, uint32_t>(3, 33),
                                              make_pair<int64_t, uint32_t>(5, 1),
                                              make_pair<int64_t, uint32_t>(2, 22)));
}

TEST_F(LookupJoinKernelTest, testSingleValueRowDeduplicateHints) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValueRowDeduplicateHintsFunc(hint);
    }
}

void LookupJoinKernelTest::testMulitiValueRowDeduplicateHintsFunc(const string &hint) {
    prepareAttributeJoinMultiValueRowDeduplicate();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<string> subPaths = {"oh", "no", "oh no no no no no"};
    vector<string> subPaths1 = {"oh", "yeah", "oh yeah yeah yeah yeah yeah"};
    vector<string> subPaths2 = {"oh", "yeah", "no", "helloworld"};
    vector<uint32_t> values = {0, 1, 0};
    vector<MatchDoc> leftDocs;
    createMultiStringDocs(leftDocs, subPaths, subPaths1, subPaths2, values);
    auto rightTable = createTable(_allocator, leftDocs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 3}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 3);
    ASSERT_EQ(colVec[1]->getIndexName(), "attr");
    ASSERT_EQ(colVec[1]->toString(), "ColumnTerm:[]:[attr:2,1,]");
    ASSERT_EQ(colVec[0]->getIndexName(), "path");
    auto castTerm = dynamic_cast<ColumnTermTyped<autil::MultiChar> *>(colVec[0]);
    ASSERT_TRUE(castTerm);
    ASSERT_THAT(castTerm->getValues(),
                testing::UnorderedElementsAre(string("oh yeah yeah yeah yeah yeah"),
                                              string("yeah"),
                                              string("yeah"),
                                              string("oh"),
                                              string("oh"),
                                              string("oh no no no no no"),
                                              string("helloworld"),
                                              string("no")));
    ASSERT_EQ(vector<size_t>({0, 3, 8}), castTerm->getOffsets());

    ASSERT_EQ(colVec[2]->getIndexName(), "aid");
    ASSERT_EQ(colVec[2]->toString(), "ColumnTerm:[]:[aid:1,0,]");
}

TEST_F(LookupJoinKernelTest, testMulitiValueRowDeduplicateHints) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMulitiValueRowDeduplicateHintsFunc(hint);
    }
}

void LookupJoinKernelTest::testSingleValueRowDeduplicateHintsForSingleColumnFunc(
    const string &hint) {
    prepareAttributeJoinRowDeduplicateForSingleColumn();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 6);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
        _allocator, docs, "fid", {1, 22, 33, 1, 22, 1}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
        _allocator, docs, "fattr", {5, 2, 3, 3, 2, 5}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 6}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 1);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    auto castTerm = dynamic_cast<ColumnTermTyped<int64_t> *>(colVec[0]);
    ASSERT_TRUE(castTerm);
    ASSERT_THAT(castTerm->getValues(), testing::UnorderedElementsAre(2, 3, 5));
    ASSERT_EQ(0, colVec[0]->getOffsets().size());
}

TEST_F(LookupJoinKernelTest, testSingleValueRowDeduplicateHintsForSingleColumn) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testSingleValueRowDeduplicateHintsForSingleColumnFunc(hint);
    }
}

void LookupJoinKernelTest::testMulitiValueRowDeduplicateHintsForSingleColumnFunc(
    const string &hint) {
    prepareAttributeJoinMultiValueRowDeduplicateForSingleColumn();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<string> subPaths = {"oh", "no", "oh no no no no no"};
    vector<string> subPaths1 = {"oh", "yeah", "oh yeah yeah yeah yeah yeah"};
    vector<string> subPaths2 = {"oh", "yeah", "no", "helloworld"};
    vector<uint32_t> values = {0, 1, 0};
    vector<MatchDoc> leftDocs;
    createMultiStringDocs(leftDocs, subPaths, subPaths1, subPaths2, values);
    auto rightTable = createTable(_allocator, leftDocs);
    auto tablePtr = dynamic_pointer_cast<TableData>(rightTable)->getTable();
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_TRUE(lookupJoinKernel->_useMatchedRow);
    auto tableQueryPtr = dynamic_pointer_cast<isearch::common::TableQuery>(
        lookupJoinKernel->genTableQuery({tablePtr, 0, 3}));
    ASSERT_TRUE(tableQueryPtr.get());
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 1);
    ASSERT_EQ(colVec[0]->getIndexName(), "path");
    auto castTerm = dynamic_cast<ColumnTermTyped<autil::MultiChar> *>(colVec[0]);
    ASSERT_TRUE(castTerm);
    ASSERT_THAT(castTerm->getValues(),
                testing::UnorderedElementsAre(string("helloworld"),
                                              string("yeah"),
                                              string("oh no no no no no"),
                                              string("oh"),
                                              string("oh yeah yeah yeah yeah yeah"),
                                              string("no")));
    ASSERT_EQ(vector<size_t>({0, 6}), castTerm->getOffsets());
}

TEST_F(LookupJoinKernelTest, testMulitiValueRowDeduplicateHintsForSingleColumn) {
    vector<string> hints {R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMulitiValueRowDeduplicateHintsForSingleColumnFunc(hint);
    }
}

void LookupJoinKernelTest::testMultiValueJoinFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 1);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0}));
    vector<string> subPaths = {"/", "/a", "/a/b"};
    char *buf = MultiValueCreator::createMultiStringBuffer(subPaths, _poolPtr.get());
    MultiString multiPath(buf);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiString>(
        _allocator, leftDocs, "path", {multiPath}));
    auto leftTable = createTable(_allocator, leftDocs);
    ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "0", "0"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelTest, testMultiValueJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMultiValueJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testMultiFieldsJoinFunc(const string &hint) {
    prepareAttributeMultiFiledJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    cout << TableUtil::toString(outputTable, 5) << endl;
    vector<string> expFid = {"0", "0", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b/c"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
    vector<string> attr = {"1", "1", "2"};
    checkOutput(attr, odata, "attr");
    checkOutput(attr, odata, "attr0");
}

TEST_F(LookupJoinKernelTest, testMultiFieldsJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMultiFieldsJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testEmptyMultiFieldsJoinFunc(const string &hint) {
    prepareAttributeMultiFiledJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInput(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(0, outputTable->getRowCount());
}

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testEmptyMultiFieldsJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testMultiFieldsJoinWithQueryFunc(const string &hint) {
    prepareAttributeMultiFiledJoinHasQuery();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA"};
    checkOutput(expGroup, odata, "group");
    vector<string> attr = {"1"};
    checkOutput(attr, odata, "attr");
    checkOutput(attr, odata, "attr0");
}

TEST_F(LookupJoinKernelTest, testMultiFieldsJoinWithQuery) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMultiFieldsJoinWithQueryFunc(hint);
    }
}

void LookupJoinKernelTest::testMultiFieldsJoinWithFilterFunc(const string &hint) {
    prepareAttributeMultiFiledJoinHasFilter();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b/c"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupB"};
    checkOutput(expGroup, odata, "group");
    vector<string> attr = {"2"};
    checkOutput(attr, odata, "attr");
    checkOutput(attr, odata, "attr0");
}

TEST_F(LookupJoinKernelTest, testMultiFieldsJoinWithFilter) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testMultiFieldsJoinWithFilterFunc(hint);
    }
}

TEST_F(LookupJoinKernelTest, testSingleValIndexSemiJoin) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({0, 3, 6, 10}, {"/a", "/a/b", "/a/c", "/b/c"}, true));

    ASSERT_NO_FATAL_FAILURE(
        checkIndexSemiOutput({0, 3, 6}, {"/a", "/a/b/c", "/a/c"}, {"groupA", "groupB", "groupB"}));
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftSemiJoin) {
    prepareAttributeSemiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({0, 3, 6, 10}, {"/a", "/a/b", "/a/c", "/b/c"}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 3, 6}, {"/a", "/a/b", "/a/c"}));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexSemiJoinBatchOutput) {
    prepareAttributeSemiJoin();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));

    ASSERT_NO_FATAL_FAILURE(checkIndexSemiOutput(
        {0, 1, 2, 3, 4, 5, 6},
        {"/a", "/a/b", "/a/b", "/a/b/c", "/a/d/c", "/a/c/c", "/a/c"},
        {"groupA", "groupA", "groupB", "groupB", "groupA", "groupA", "groupB"}));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftSemiJoinBatchOutput) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));

    ASSERT_NO_FATAL_FAILURE(
        checkSemiOutput({0, 1, 2, 3, 4, 5, 6}, {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b"}));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftSemiJoinBatchOutput2) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));

    ASSERT_NO_FATAL_FAILURE(
        checkSemiOutput({0, 1, 2, 3, 4, 5, 6}, {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b"}));
}

void LookupJoinKernelTest::testMultiFieldsSemiJoinFunc(const string &hint) {
    prepareAttributeMultiFiledSemiJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputMultiColumn(outputTable, "path", {{"/a", "/a/b/c", "/a/b"}, {"/a/b/c"}}));
}

TEST_F(LookupJoinKernelTest, testMultiFieldsSemiJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testMultiFieldsSemiJoinFunc(hint));
    }
}

void LookupJoinKernelTest::testEmptyMultiFieldsSemiJoinFunc(const string &hint) {
    prepareAttributeMultiFiledSemiJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInput(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(0, outputTable->getRowCount());
}

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsSemiJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testEmptyMultiFieldsSemiJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testMultiFieldsAntiJoinFunc(const string &hint) {
    prepareAttributeMultiFiledSemiJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_NO_FATAL_FAILURE(getOutputTable(testerPtr, outputTable, eof));
    ASSERT_TRUE(eof);

    ASSERT_EQ(1, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(outputTable, "path", {{}}));
}

TEST_F(LookupJoinKernelTest, testMultiFieldsAntiJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testMultiFieldsAntiJoinFunc(hint));
    }
}

void LookupJoinKernelTest::testEmptyMultiFieldsAntiJoinFunc(const string &hint) {
    prepareAttributeMultiFiledSemiJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInput(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(outputTable, "path", {{}, {}, {}}));
}

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsAntiJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testEmptyMultiFieldsAntiJoinFunc(hint);
    }
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftAntiJoin) {
    prepareAttributeAntiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({0, 3, 6, 10}, {"/a", "/a/b", "/a/c", "/b/c"}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10}, {"/b/c"}));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftAntiJoinBatchOutput) {
    prepareAttributeAntiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 10, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 7, 8, 9}, {"/c", "/c", "/a", "/b"}));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftAntiJoinBatch) {
    prepareAttributeAntiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 10, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 7, 8, 9}, {"/c", "/c", "/a", "/b"}));
}

void LookupJoinKernelTest::testPartialIndexJoinLeftSemiJoinFunc(const string &hint) {
    preparePartialIndexSemiJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "aid", {0, 3, 6, 10}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "group", {"groupA", "groupB", "groupA", "groupB"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expAid = {"0", "3"};
    checkOutput(expAid, odata, "aid");
    vector<string> expPath = {"/a", "/a/b/c"};
    checkOutput(expPath, odata, "path");
    vector<string> expGroup = {"groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelTest, testPartialIndexJoinLeftSemiJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"group"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid,group"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPartialIndexJoinLeftSemiJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testNotEqualJoinFunc(const string &hint) {
    prepareAttributeMultiFiledJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["is_equi_join"] = true;
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$attr", "$attr0"]})json");
    _attributeMap["equi_condition"]
        = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");

    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "0"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b", "/a/b/c"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupB", "groupB"};
    checkOutput(expGroup, odata, "group");
    vector<string> attr = {"1", "1"};
    vector<string> attr2 = {"2", "2"};
    checkOutput(attr, odata, "attr");
    checkOutput(attr2, odata, "attr0");
}

TEST_F(LookupJoinKernelTest, testNotEqualJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testNotEqualJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testLeftJoinFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
    auto leftTable = createTable(_allocator, leftDocs);
    ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(3, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {1, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b", "/a/b", "/a"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b", "/a/b", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupA", "groupB", ""}));
    TablePtr inputTable = getTable(leftTable);
    ASSERT_TRUE(inputTable);
    checkDependentTable(inputTable, outputTable);
}

TEST_F(LookupJoinKernelTest, testLeftJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testLeftJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testLeftJoinSingleEmptyFunc(const string &hint) {
    prepareAttributeMultiFiledJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("LEFT");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInputPath(testerPtr);

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(4, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(
        outputTable, "path", {{"/a", "/a/b/c", "/a/b"}, {"/a", "/a/b/c", "/a/b"}, {"/a/b/c"}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a", "/a/b", "/a/b/c", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", {0, 1, 3, 0}));
}

TEST_F(LookupJoinKernelTest, testLeftJoinSingleEmpty) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testLeftJoinSingleEmptyFunc(hint);
    }
}

void LookupJoinKernelTest::testLeftJoinAllEmptyFunc(const string &hint) {
    prepareAttributeMultiFiledJoin();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("LEFT");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    checkTableInput(testerPtr);

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(3, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(outputTable, "path", {{}, {}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"", "", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", {0, 0, 0}));
}

TEST_F(LookupJoinKernelTest, testLeftJoinAllEmpty) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr,path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testLeftJoinAllEmptyFunc(hint);
    }
}

void LookupJoinKernelTest::testLeftJoinBatchInputFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
    }
    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {1, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b", "/a/b", "/a"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b", "/a/b", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupA", "groupB", ""}));

    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {2, 0, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a/b/c", "/a/c", "/a/c/c"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {2, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b/c", "/a/c/c", "/a/c"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b/c", "/a/c/c", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupB", "groupA", ""}));
}

TEST_F(LookupJoinKernelTest, testLeftJoinBatchInput) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testLeftJoinBatchInputFunc(hint);
    }
}

void LookupJoinKernelTest::testLeftJoinBatchOutputFunc(const string &hint) {
    prepareAttribute();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {0, 1, 2, 0, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/a/b/c", "/a/c", "/a/c/c"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }

    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
    {
        vector<uint32_t> expFid = {1, 1, 2, 1, 0, 0};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "fid", expFid));
        vector<string> expPath = {"/a/b", "/a/b", "/a/b/c", "/a/c/c", "/a", "/a/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", expPath));
        vector<string> expPath0 = {"/a/b", "/a/b", "/a/b/c", "/a/c/c", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", expPath0));
        vector<string> expGroup = {"groupA", "groupB", "groupB", "groupA", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", expGroup));
    }
}

TEST_F(LookupJoinKernelTest, testLeftJoinBatchOutput) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"path"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        ASSERT_NO_FATAL_FAILURE(testLeftJoinBatchOutputFunc(hint));
    }
}

TEST_F(LookupJoinKernelTest, testSemiJoinBatchInput) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(
        prepareLeftTable({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                         {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                         true));

    ASSERT_NO_FATAL_FAILURE(
        checkSemiOutput({0, 1, 2, 3, 4, 5, 6}, {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b"}));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinInner) {
    prepareAttribute();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({}, {}, true));
    ASSERT_NO_FATAL_FAILURE(checkInnerOutput({}, {}, {}, {}, {}, true));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinLeft) {
    prepareAttribute();
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({}, {}, true));
    ASSERT_NO_FATAL_FAILURE(checkInnerOutput({}, {}, {}, {}, {}, true));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinSemi) {
    prepareAttributeSemiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({}, {}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinAnti) {
    prepareAttributeAntiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({}, {}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}));
}

void LookupJoinKernelTest::testPKJoinFunc(const string &hint) {
    prepareAttributePKJoinOptimize();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {4, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "id", {-1, 3}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"1", "4"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b/c", "/a/b/c/d"};
    checkOutput(expPath, odata, "path");
    vector<string> expAid = {"3", "-1"};
    checkOutput(expAid, odata, "id");
    checkOutput(expAid, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testPKJoin) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPKJoinFunc(hint);
    }
}

void LookupJoinKernelTest::testPKJoinRightIsBuildFunc(const string &hint) {
    prepareAttributePKJoinOptimizeRightIsBuild();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int64_t>(
            _allocator, docs, "id", {{1, 2, 7}, {6, 8}}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"2", "2", "3"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b", "/a/b", "/a/c"};
    checkOutput(expPath, odata, "path");
    vector<string> expAid = {"1", "2", "6"};
    checkOutput(expAid, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testPKJoinRightIsBuild) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPKJoinRightIsBuildFunc(hint);
    }
}

void LookupJoinKernelTest::testNumberJoinRightIsBuildFunc(const string &hint) {
    prepareAttributeNumberJoinOptimizeRightIsBuild();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"22", "22", "33", "22"};
    checkOutput(expFid, odata, "fid");
    vector<string> expAid = {"2", "3", "4", "6"};
    checkOutput(expAid, odata, "aid");
    vector<string> expAttr = {"2", "2", "3", "2"};
    checkOutput(expAttr, odata, "attr");
    checkOutput(expAttr, odata, "fattr");
}

TEST_F(LookupJoinKernelTest, testNumberJoinRightIsBuild) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"attr"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testNumberJoinRightIsBuildFunc(hint);
    }
}

TEST_F(LookupJoinKernelTest, testGetDocidField_fieldSizeError) {
    string ret;
    ASSERT_FALSE(LookupJoinKernel::getDocIdField({}, {}, ret));
}

TEST_F(LookupJoinKernelTest, testGetDocidField_FieldNotInnerDocId) {
    string ret;
    ASSERT_FALSE(LookupJoinKernel::getDocIdField({"a"}, {"b"}, ret));
}

TEST_F(LookupJoinKernelTest, testGetDocidField) {
    string ret;
    ASSERT_TRUE(LookupJoinKernel::getDocIdField({"a"}, {string(INNER_DOCID_FIELD_NAME)}, ret));
    ASSERT_EQ("a", ret);

    LookupJoinKernel kernel;
    kernel._leftJoinColumns = {INNER_DOCID_FIELD_NAME};
    kernel._rightJoinColumns = {"a"};
    kernel._leftTableIndexed = true;
    ASSERT_TRUE(kernel.getDocIdField(kernel._rightJoinColumns, kernel._leftJoinColumns, ret));
    ASSERT_EQ("a", ret);
    kernel._leftTableIndexed = false;
    ASSERT_FALSE(kernel.getDocIdField(kernel._leftJoinColumns, kernel._rightJoinColumns, ret));

    kernel._leftJoinColumns = {"a"};
    kernel._rightJoinColumns = {INNER_DOCID_FIELD_NAME};
    ASSERT_TRUE(kernel.getDocIdField(kernel._leftJoinColumns, kernel._rightJoinColumns, ret));
    ASSERT_EQ("a", ret);
}

TEST_F(LookupJoinKernelTest, testGenDocids_ColumnNotFound) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupJoinKernel::genDocIds({input, 2, 3}, "a", docIds));
    CHECK_TRACE_COUNT(1, "invalid join column name [a]", provider.getTrace(""));
}

TEST_F(LookupJoinKernelTest, testGenDocids_joinTypeNotSupport) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupJoinKernel::genDocIds({input, 2, 3}, "fid", docIds));
    CHECK_TRACE_COUNT(1, "inner docid join only support int32_t", provider.getTrace(""));
}

TEST_F(LookupJoinKernelTest, testGenDocids_joinTypeNotSupport2) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupJoinKernel::genDocIds({input, 2, 3}, "ids", docIds));
    CHECK_TRACE_COUNT(1, "inner docid join only support int32_t", provider.getTrace(""));
}

TEST_F(LookupJoinKernelTest, testGenDocids_Empty) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_TRUE(LookupJoinKernel::genDocIds({input, 1, 0}, "docid", docIds));
    ASSERT_TRUE(docIds.empty());
}

TEST_F(LookupJoinKernelTest, testGenDocids_Success) {
    navi::NaviLoggerProvider provider;
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_TRUE(LookupJoinKernel::genDocIds({input, 0, 1}, "docid", docIds));
    vector<docid_t> expected = {2};
    ASSERT_EQ(expected, docIds);
}

TEST_F(LookupJoinKernelTest, testDocIds_Join) {
    prepareAttribute();
    _attributeMap["equi_condition"] = _attributeMap["condition"]
        = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$__inner_docid"]})json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$id", "$fid", "$path", "$aid", "$group", "$__inner_docid"])json");
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$id", "$fid", "$path"])json");
    _attributeMap["right_input_fields"]
        = ParseJson(R"json(["$aid", "$group", "$__inner_docid"])json");
    _attributeMap["build_node"] = ParseJson(
        R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields_internal":["$aid", "$group", "$__inner_docid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");

    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "id", {0, 3}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    navi::NaviLoggerProvider provider("DEBUG");
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "1"};
    ASSERT_NO_FATAL_FAILURE(checkOutput(expFid, odata, "fid"));
    vector<string> expPath = {"/a", "/a/b"};
    ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
    vector<string> aid = {"0", "3"};
    ASSERT_NO_FATAL_FAILURE(checkOutput(aid, odata, "aid"));
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
    ASSERT_FALSE(lookupJoinKernel->_useMatchedRow);
}

TEST_F(LookupJoinKernelTest, testDocidsJoin_ReuseInput) {
    // reuse is empty
    {
        prepareAttribute();
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op":"=", "type":"OTHER", "params":["$id", "$__inner_docid"]})json");
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$id", "$fid", "$path", "$aid", "$group", "$__inner_docid"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$id", "$fid", "$path"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$group", "$__inner_docid"])json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields_internal":["$aid", "$group", "$__inner_docid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");

        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        DataPtr leftTable;
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
            ASSERT_NO_FATAL_FAILURE(
                _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "id", {0, 3}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "fid", {0, 1}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b"}));
            leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }
        ASSERT_TRUE(testerPtr->compute());
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expFid = {"0", "1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a", "/a/b"};
        checkOutput(expPath, odata, "path");
        vector<string> aid = {"0", "3"};
        checkOutput(aid, odata, "aid");
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "id", {0, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(getTable(leftTable), "path", {"/a", "/a/b"}));
        auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
        ASSERT_FALSE(lookupJoinKernel->_useMatchedRow);
    }

    // reuse input
    {
        prepareAttribute();
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op":"=", "type":"OTHER", "params":["$id", "$__inner_docid"]})json");
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$id", "$fid", "$path", "$aid", "$group", "$__inner_docid"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$id", "$fid", "$path"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$group", "$__inner_docid"])json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields_internal":["$aid", "$group", "$__inner_docid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");

        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        DataPtr leftTable;
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
            ASSERT_NO_FATAL_FAILURE(
                _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "id", {0, 3}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "fid", {0, 1}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b"}));
            leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }
        ASSERT_TRUE(testerPtr->compute());
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expFid = {"0", "1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a", "/a/b"};
        checkOutput(expPath, odata, "path");
        vector<string> aid = {"0", "3"};
        checkOutput(aid, odata, "aid");
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "id", {0, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(getTable(leftTable), "path", {"/a", "/a/b"}));
        auto lookupJoinKernel = dynamic_cast<LookupJoinKernel *>(testerPtr->getKernel());
        ASSERT_FALSE(lookupJoinKernel->_useMatchedRow);
    }
}

TEST_F(LookupJoinKernelTest, testMatchRowIndexOptimizeEnabled) {
    prepareAttributeNumberJoinLargeBatch();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"22", "22", "33", "22"};
    checkOutput(expFid, odata, "fid");
    vector<string> expAid = {"2", "3", "4", "6"};
    checkOutput(expAid, odata, "aid");
    vector<string> expAttr = {"2", "2", "3", "2"};
    checkOutput(expAttr, odata, "attr");
    checkOutput(expAttr, odata, "fattr");
}

TEST_F(LookupJoinKernelTest, testMatchRowIndexOptimizeDisabled) {
    prepareAttributeNumberJoinLargeBatch();
    _attributeMap["hints"]
        = ParseJson(R"json({"JOIN_ATTR":{"disableMatchRowIndexOptimize":"yes"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable, true));
    }

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"22", "22", "22", "33"};
    checkOutput(expFid, odata, "fid");
    vector<string> expAid = {"2", "3", "6", "4"};
    checkOutput(expAid, odata, "aid");
    vector<string> expAttr = {"2", "2", "2", "3"};
    checkOutput(expAttr, odata, "attr");
    checkOutput(expAttr, odata, "fattr");
}

class LookupJoinKernelStringPKTest : public LookupJoinKernelTest {
public:
    void prepareInvertedTableData(string &tableName,
                                  string &fields,
                                  string &indexes,
                                  string &attributes,
                                  string &summarys,
                                  string &truncateProfileStr,
                                  string &docs,
                                  int64_t &ttl) override {
        _tableName = "item";
        tableName = _tableName;
        fields = "uuid:string;name:string";
        indexes = "uuid:primarykey64:uuid;name:string:name";
        attributes = "uuid;name";
        summarys = "uuid;name";
        truncateProfileStr = "";
        docs = "cmd=add,uuid=a111,name=aaa;"
               "cmd=add,uuid=b222,name=bbb;"
               "cmd=add,uuid=c333,name=ccc;"
               "cmd=add,uuid=d444,name=ddd;"
               "cmd=add,uuid=e555,name=eee;"
               "cmd=add,uuid=f666,name=fff;"
               "cmd=add,uuid=g777,name=ggg;";
        ttl = numeric_limits<int64_t>::max();
    }

    void prepareAttributeStringPKJoinOptimize() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cate_id"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$uuid", "$name"])json");
        _attributeMap["right_table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "int32",
                "field_name": "$uuid",
                "index_name": "uuid",
                "index_type": "primarykey64"
            },
            {
                "field_type": "string",
                "field_name": "$name",
                "index_name": "name",
                "index_type": "STRING"
            }
        ]
        })json");

        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$uid", "$cate_id", "$uuid", "$name"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$uuid", "$uid"]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"item", "db_name":"default", "catalog_name":"default", "index_infos": {"$uuid":{"type":"primarykey64", "name":"uuid"}, "$name":{"type":"STRING", "name":"name"}}, "output_fields_internal":["$uuid", "$name"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$uuid"]})json");
    }

    void testPKJoin1Func(const string &hint);
    void testPKJoin2Func(const string &hint);
};

void LookupJoinKernelStringPKTest::testPKJoin1Func(const string &hint) {
    prepareAttributeStringPKJoinOptimize();
    if (!hint.empty()) {
        _attributeMap["hints"] = ParseJson(hint);
    }
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        vector<string> inputUids = {"a111", "d444", "h888"};
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, docs, "uid", inputUids));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "cate_id", {1, 4, 8}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"a111", "d444"};
    checkOutput(expFid, odata, "uid");
    checkOutput(expFid, odata, "uuid");
    vector<string> expPath = {"1", "4"};
    checkOutput(expPath, odata, "cate_id");
    vector<string> expAid = {"aaa", "ddd"};
    checkOutput(expAid, odata, "name");
}

TEST_F(LookupJoinKernelStringPKTest, testPKJoin1) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"uid"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPKJoin1Func(hint);
    }
}

void LookupJoinKernelStringPKTest::testPKJoin2Func(const string &hint) {
    prepareAttributeStringPKJoinOptimize();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 2);
        vector<vector<string>> inputUids = {{"b222", "c333"}, {"f666", "h888"}};
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMultiValueMatchDocAllocator(_allocator, docs, "uid", inputUids));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "cate_id", {1, 4}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"b222", "c333", "f666"};
    checkOutput(expFid, odata, "uuid");
    vector<string> expPath = {"1", "1", "4"};
    checkOutput(expPath, odata, "cate_id");
    vector<string> expAid = {"bbb", "ccc", "fff"};
    checkOutput(expAid, odata, "name");
}

TEST_F(LookupJoinKernelStringPKTest, testPKJoin2) {
    vector<string> hints {{},
                          R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"uid"}})json",
                          R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json"};
    for (const auto &hint : hints) {
        testPKJoin2Func(hint);
    }
}

TEST_F(LookupJoinKernelTest, testSelectValidField) {
    TablePtr input = prepareTable();
    vector<string> inputFields;
    vector<string> joinFields;
    bool ret = LookupJoinKernel::selectValidField(input, inputFields, joinFields, false);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());

    vector<string> expectInput = {"fid", "path"};
    vector<string> expectJoin = {"a", "b", "c"};
    inputFields = expectInput;
    joinFields = expectJoin;
    ret = LookupJoinKernel::selectValidField(input, inputFields, joinFields, false);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());
    ASSERT_EQ(expectInput[0], inputFields[1]);
    ASSERT_EQ(expectInput[1], inputFields[0]);
    ASSERT_EQ(expectJoin[0], joinFields[1]);
    ASSERT_EQ(expectJoin[1], joinFields[0]);

    inputFields = expectInput;
    joinFields = expectJoin;
    ret = LookupJoinKernel::selectValidField(input, inputFields, joinFields, true);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());
    ASSERT_EQ(expectInput[0], inputFields[0]);
    ASSERT_EQ(expectInput[1], inputFields[1]);
    ASSERT_EQ(expectJoin[0], joinFields[0]);
    ASSERT_EQ(expectJoin[1], joinFields[1]);
}

TEST_F(LookupJoinKernelTest, testGenStreamQueryTerm) {
    TablePtr input = prepareTable();
    vector<string> termVec;
    Row row;
    row.docid = 0;
    row.index = 0;
    string field = "fid";
    bool ret = LookupJoinKernel::genStreamQueryTerm(input, row, field, termVec);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(termVec.empty());
    ASSERT_EQ("1", termVec[0]);
    termVec.clear();

    string mfield = "ids";
    ret = LookupJoinKernel::genStreamQueryTerm(input, row, mfield, termVec);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(termVec.empty());
    ASSERT_EQ("1", termVec[0]);
    ASSERT_EQ("2", termVec[1]);
    termVec.clear();
}

TEST_F(LookupJoinKernelTest, testGetPkTriggerField) {
    vector<string> inputFields = {"t1", "t2", "t3"};
    vector<string> joinFields = {"f1", "f2", "f3", "f4"};
    map<string, sql::IndexInfo> indexInfoMap;
    string expectField = "null";
    string triggerField = expectField;
    indexInfoMap["pk1"] = sql::IndexInfo("f1", "long");
    indexInfoMap["pk2"] = sql::IndexInfo("ff2", "primary_key");
    indexInfoMap["pk3"] = sql::IndexInfo("f4", "primarykey128");
    bool ret
        = LookupJoinKernel::getPkTriggerField(inputFields, joinFields, indexInfoMap, triggerField);
    ASSERT_FALSE(ret);
    ASSERT_EQ(expectField, triggerField);

    indexInfoMap["pk2"] = sql::IndexInfo("f2", "primarykey64");
    ret = LookupJoinKernel::getPkTriggerField(inputFields, joinFields, indexInfoMap, triggerField);
    ASSERT_TRUE(ret);
    ASSERT_NE(expectField, triggerField);
    ASSERT_EQ("t2", triggerField);
}

TEST_F(LookupJoinKernelTest, testDocIdsJoin) {
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
    auto leftData = createTable(_allocator, leftDocs);
    auto leftTable = getTable(leftData);
    vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
    auto rightData = createTable(_allocator, rightDocs);
    auto rightTable = getTable(rightData);
    {
        LookupJoinKernel kernel;
        ASSERT_TRUE(kernel.docIdsJoin({leftTable, 0, 2}, rightTable));
        ASSERT_EQ(2, kernel._tableAIndexes.size());
        ASSERT_EQ(0, kernel._tableAIndexes[0]);
        ASSERT_EQ(1, kernel._tableAIndexes[1]);

        ASSERT_EQ(2, kernel._tableBIndexes.size());
        ASSERT_EQ(0, kernel._tableBIndexes[0]);
        ASSERT_EQ(1, kernel._tableBIndexes[1]);
    }
    {
        LookupJoinKernel kernel;
        ASSERT_TRUE(kernel.docIdsJoin({leftTable, 1, 2}, rightTable));
        ASSERT_EQ(2, kernel._tableAIndexes.size());
        ASSERT_EQ(1, kernel._tableAIndexes[0]);
        ASSERT_EQ(2, kernel._tableAIndexes[1]);

        ASSERT_EQ(2, kernel._tableBIndexes.size());
        ASSERT_EQ(0, kernel._tableBIndexes[0]);
        ASSERT_EQ(1, kernel._tableBIndexes[1]);
    }
    {
        LookupJoinKernel kernel;
        ASSERT_FALSE(kernel.docIdsJoin({leftTable, 0, 3}, rightTable));
    }
    {
        LookupJoinKernel kernel;
        ASSERT_TRUE(kernel.docIdsJoin({leftTable, 2, 2}, rightTable));
    }
    {
        LookupJoinKernel kernel;
        ASSERT_FALSE(kernel.docIdsJoin({leftTable, 3, 1}, rightTable));
    }
}

} // namespace sql
