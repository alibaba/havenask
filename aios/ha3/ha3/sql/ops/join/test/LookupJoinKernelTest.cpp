#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/data/TableSchema.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

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
    void prepareAttribute() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeLeftBuildIsTrue() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$path", "$group","$fid", "$path0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void preparePartialIndexJoinLeftBuildIsTrue() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$path", "$group"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$path", "$group","$fid", "$path0", "$group0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$path","$path0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void preparePartialIndexSemiJoin() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$group"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"] =
            ParseJson(R"json(["$aid", "$path", "$group","$aid0","$group0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$aid","$aid0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }


    void prepareAttributeSemiJoin() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid"])json");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"] =
            ParseJson(R"json(["$aid", "$path", "$group","$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":5, "hash_fields":["$id"]})json");
    }

    void prepareAttributeSemiJoinRightIsBuild() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":5, "hash_fields":["$id"]})json");
        _attributeMap["left_is_build"] = false;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"] =
            ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
    }

    void prepareAttributeAntiJoinRightIsBuild() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["join_type"] = string("ANTI");
        _attributeMap["semi_join_type"] = string("ANTI");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$aid", "$fid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":5, "hash_fields":["$id"]})json");
        _attributeMap["left_is_build"] = false;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["output_fields_internal"] =
            ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
    }

    void prepareAttributeMultiFiledJoin() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"},"$attr":{"type":"primarykey64", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeMultiFiledSemiJoin() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("SEMI");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"},"$attr":{"type":"NUMBER", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr"])json");
        _attributeMap["output_fields_internal"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeMultiFiledJoinHasQuery() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"},"$attr":{"type":"NUMBER", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "condition":{"op":"=", "params":["$aid", 1]},"batch_size":1, "hash_fields": ["$id"]})json");
    }

    void prepareAttributeMultiFiledJoinHasFilter() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"},"$attr":{"type":"primarykey64", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "condition":{"op":">", "params":["$aid", 1]},"batch_size":1, "hash_fields": ["$id"]})json");
    }

    void prepareAttributePKJoinOptimize() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$id"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$id", "$aid", "$path"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$aid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributePKJoinOptimizeRightIsBuild() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fid", "$id"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$path", "$fid", "$id"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$aid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeNumberJoinOptimizeRightIsBuild() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$attr", "$fattr"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$attr", "$fid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeJoinCache() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"primarykey64", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]},{"op":"=","params":["$aid", "$fid"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"primarykey64", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$attr", "$fid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeJoinRowDeduplicate() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]},{"op":"=","params":["$aid", "$fid"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$attr", "$fid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeJoinMultiValueRowDeduplicate() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"},"$attr":{"type":"NUMBER", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$attr0"]},{"op":"=","params":["$path", "$path0"]},{"op":"=","params":["$fid", "$aid"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeJoinRowDeduplicateForSingleColumn() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = true;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$fattr", "$fid"])json");
        _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$aid", "$attr", "$fattr", "$fid"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$attr", "$fattr"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$attr", "$fid"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeJoinMultiValueRowDeduplicateForSingleColumn() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$attr"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group", "$attr"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"},"$attr":{"type":"NUMBER", "name":"attr"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path","$attr", "$aid", "$path0", "$group", "$attr0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op": "AND", "params":[{"op":"=","params":["$path", "$path0"]}]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"},"$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group", "$attr"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareInvertedTableData(string &tableName,
                                  string &fields, string &indexes, string &attributes,
                                  string &summarys, string &truncateProfileStr,
                                  string &docs, int64_t &ttl) override
    {
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
               "cmd=add,aid=6,path=/a/c,attr=2,group=groupB;";
        ttl = numeric_limits<int64_t>::max();
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }

    TablePtr prepareTable() {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 1));
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {1}));
        IF_FAILURE_RET_NULL(extendMatchDocAllocator(_allocator, leftDocs, "path", {"abc"}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "pathM", {multiPath}));
        vector<int32_t> ids = {1, 2};
        buf = MultiValueCreator::createMultiValueBuffer(ids.data(), 2, &_pool);
        MultiInt32 multiIds;
        multiIds.init(buf);
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<MultiInt32>(_allocator, leftDocs, "ids", {multiIds}));
        TablePtr table(new Table(leftDocs, _allocator));
        return table;
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount);
        ColumnPtr column = outputTable->getColumn(field);
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
        builder.kernel("LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        _testerPtr.reset(builder.build());
        ASSERT_TRUE(_testerPtr.get());
        ASSERT_FALSE(_testerPtr->hasError());
    }

    void prepareLeftTable(const vector<uint32_t> &fid, const vector<string> &path, bool eof) {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, fid.size()));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator,
                        leftDocs, "fid", fid));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator,
                        leftDocs, "path", path));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(_testerPtr->setInput("input0", leftTable));
        if (eof) {
            ASSERT_TRUE(_testerPtr->setInputEof("input0"));
        }
    }

    void checkIndexSemiOutput(const vector<int64_t> &aid,
                              const vector<string> &path,
                              const vector<string> &group,
                              bool isEof,
                              bool hasOutput = true)
    {
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
        ASSERT_EQ(3, outputTable->getColumnCount());
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
                          bool hasOutput = true)
    {
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
    void checkSemiOutput(const vector<uint32_t> &fid,
                         const vector<string> &path,
                         bool isEof,
                         bool hasOutput = true)
    {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        if (hasOutput == false) {
            return;
        }
        ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
        ASSERT_EQ(isEof, eof);
        ASSERT_EQ(fid.size(), outputTable->getRowCount()) << TableUtil::toString(outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", fid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(outputTable, "path", path));
    }

    void checkNoOutput()
    {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
        TablePtr outputTable;
        bool eof = false;
        ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
    }

    void checkOutputEof(bool isEof = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
        DataPtr odata;
        bool eof;
        ASSERT_TRUE(_testerPtr->getOutput("output0", odata, eof));
        ASSERT_EQ(eof, isEof);
    }

public:
    MatchDocAllocatorPtr _allocator;
protected:
    HA3_LOG_DECLARE();
private:
    KernelTesterPtr _testerPtr;
};

HA3_LOG_SETUP(sql, LookupJoinKernelTest);


LookupJoinKernelTest::LookupJoinKernelTest() {
}

LookupJoinKernelTest::~LookupJoinKernelTest() {
}

TEST_F(LookupJoinKernelTest, testSingleValJoinRight) {
    prepareAttribute();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
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

TEST_F(LookupJoinKernelTest, testSingleValJoinRightInputIsEmpty) {
    prepareAttribute();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        _allocator.reset(new MatchDocAllocator(&_pool));
        _allocator->declare<uint32_t>("fid", SL_ATTRIBUTE);
        _allocator->declare<MultiChar>("path", SL_ATTRIBUTE);
        auto leftTable = createTable(_allocator, {});
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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
    checkOutput({}, odata, "fid");
    checkOutput({}, odata, "path");
    checkOutput({}, odata, "path0");
    checkOutput({}, odata, "group");
    checkOutput({}, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinRightScanIsEmpty) {
    prepareAttribute();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a/cc", "/a/b/cc"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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
    checkOutput({}, odata, "fid");
    checkOutput({}, odata, "path");
    checkOutput({}, odata, "path0");
    checkOutput({}, odata, "group");
    checkOutput({}, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeft) {
    prepareAttributeLeftBuildIsTrue();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testPartialIndexJoinLeft) {
    preparePartialIndexJoinLeftBuildIsTrue();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "group", {"group2A", "groupB"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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


TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithBatchLimit1) {
    prepareAttributeLeftBuildIsTrue();
    _attributeMap["batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"0"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(0, outputTable->getRowCount());
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithLookupBatchLimit1) {
    prepareAttributeLeftBuildIsTrue();
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithLimit1) {
    prepareAttributeLeftBuildIsTrue();
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"0"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_FALSE(eof);
    {
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
    eof = false;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(0, outputTable->getRowCount());
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftWithThresholdHints) {
    prepareAttributeLeftBuildIsTrue();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupTurncateThreshold":"1","lookupBatchSize":"10"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "path", {"/a", "/a/b"}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

TEST_F(LookupJoinKernelTest, testCacheDisableHints) {
    prepareAttributeJoinCache();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid,attr"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 2));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_FALSE(colVec[0]->getEnableCache());
    ASSERT_EQ(colVec[1]->getIndexName(), "aid");
    ASSERT_FALSE(colVec[1]->getEnableCache());
}

TEST_F(LookupJoinKernelTest, testCacheDisableHints2) {
    prepareAttributeJoinCache();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupDisableCacheFields":"aid"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 2));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_TRUE(colVec[0]->getEnableCache());
    ASSERT_EQ(colVec[1]->getIndexName(), "aid");
    ASSERT_FALSE(colVec[1]->getEnableCache());
}

TEST_F(LookupJoinKernelTest, testSingleValueRowDeduplicateHints) {
    prepareAttributeJoinRowDeduplicate();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 6));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {1, 22, 33, 1, 22, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {5, 2, 3, 3, 2, 5}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 6));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 2);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_EQ(colVec[0]->toString(), "ColumnTerm:[]:[attr:3,3,5,2,]");
    ASSERT_EQ(colVec[1]->getIndexName(), "aid");
    ASSERT_EQ(colVec[1]->toString(), "ColumnTerm:[]:[aid:1,33,1,22,]");
}

TEST_F(LookupJoinKernelTest, testMulitiValueRowDeduplicateHints) {
    prepareAttributeJoinMultiValueRowDeduplicate();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    
    vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));

    vector<string> subPaths = {"oh", "no", "oh no no no no no"};
    char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
    MultiString multiPath;
    multiPath.init(buf);

    vector<string> subPaths1 = {"oh", "yeah", "oh yeah yeah yeah yeah yeah"};
    buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
    MultiString multiPath1;
    multiPath1.init(buf);

    vector<string> subPaths2 = {"oh", "yeah", "no", "helloworld"};
    buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
    MultiString multiPath2;
    multiPath2.init(buf);

    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
    
    auto rightTable = createTable(_allocator, leftDocs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 3));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 3);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_EQ(colVec[0]->toString(), "ColumnTerm:[]:[attr:2,1,]");
    ASSERT_EQ(colVec[2]->getIndexName(), "path");
    ASSERT_EQ(colVec[2]->toString(), "ColumnTerm:[0,3,8,]:[path:oh yeah yeah yeah yeah yeah,oh,yeah,helloworld,yeah,oh no no no no no,oh,no,]");
    ASSERT_EQ(colVec[1]->getIndexName(), "aid");
    ASSERT_EQ(colVec[1]->toString(), "ColumnTerm:[]:[aid:1,0,]");
}

TEST_F(LookupJoinKernelTest, testSingleValueRowDeduplicateHintsForSingleColumn) {
    prepareAttributeJoinRowDeduplicateForSingleColumn();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 6));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {1, 22, 33, 1, 22, 1}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {5, 2, 3, 3, 2, 5}));
    auto rightTable = createTable(_allocator, docs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 6));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 1);
    ASSERT_EQ(colVec[0]->getIndexName(), "attr");
    ASSERT_EQ(colVec[0]->toString(), "ColumnTerm:[]:[attr:3,5,2,]");
}

TEST_F(LookupJoinKernelTest, testMulitiValueRowDeduplicateHintsForSingleColumn) {
    prepareAttributeJoinMultiValueRowDeduplicateForSingleColumn();
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"lookupEnableRowDeduplicate":"yes"}})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    
    vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));

    vector<string> subPaths = {"oh", "no", "oh no no no no no"};
    char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
    MultiString multiPath;
    multiPath.init(buf);

    vector<string> subPaths1 = {"oh", "yeah", "oh yeah yeah yeah yeah yeah"};
    buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
    MultiString multiPath1;
    multiPath1.init(buf);

    vector<string> subPaths2 = {"oh", "yeah", "no", "helloworld"};
    buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
    MultiString multiPath2;
    multiPath2.init(buf);

    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
    
    auto rightTable = createTable(_allocator, leftDocs);
    auto tablePtr = dynamic_pointer_cast<Table>(rightTable);
    auto lookupJoinKernel = dynamic_cast<LookupJoinKernel*>(testerPtr->getKernel());
    auto tableQueryPtr = dynamic_pointer_cast<HA3_NS(common)::TableQuery>(lookupJoinKernel->genTableQuery(tablePtr, 0, 3));
    auto colVec = tableQueryPtr->getTermArray();
    ASSERT_EQ(colVec.size(), 1);
    ASSERT_EQ(colVec[0]->getIndexName(), "path");
    ASSERT_EQ(colVec[0]->toString(), "ColumnTerm:[0,6,]:[path:helloworld,yeah,oh no no no no no,oh,oh yeah yeah yeah yeah yeah,no,]");
}

TEST_F(LookupJoinKernelTest, testMultiValueJoin) {
    prepareAttribute();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0}));
        vector<string> subPaths = {"/", "/a", "/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testMultiFieldsJoin) {
    prepareAttributeMultiFiledJoin();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    TablePtr outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != NULL);
    cout << TableUtil::toString(outputTable, 5)<< endl;
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

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsJoin) {
    prepareAttributeMultiFiledJoin();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testMultiFieldsJoinWithQuery) {
    prepareAttributeMultiFiledJoinHasQuery();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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


TEST_F(LookupJoinKernelTest, testMultiFieldsJoinWithFilter) {
    prepareAttributeMultiFiledJoinHasFilter();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testSingleValIndexSemiJoin) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 3, 6, 10},
                    {"/a", "/a/b", "/a/c", "/b/c"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkIndexSemiOutput(
            {0, 3, 6},
            {"/a", "/a/b/c", "/a/c"},
            {"groupA", "groupB", "groupB"},
            true));
}

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftSemiJoin) {
    prepareAttributeSemiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 3, 6, 10},
                    {"/a", "/a/b", "/a/c", "/b/c"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
            {0, 3, 6},
            {"/a", "/a/b", "/a/c"},
            true));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexSemiJoinBatchOutput) {
    prepareAttributeSemiJoin();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkIndexSemiOutput(
                    {0, 1, 2, 3, 4},
                    {"/a", "/a/b", "/a/b", "/a/b/c", "/a/d/c"},
                    {"groupA", "groupA", "groupB", "groupB", "groupA"},
                    false));
    ASSERT_NO_FATAL_FAILURE(checkIndexSemiOutput(
                    {5, 6},
                    {"/a/c/c", "/a/c"},
                    {"groupA", "groupB"},
                    true));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftSemiJoinBatchOutput) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {0, 1, 2, 3, 4},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b"},
                    false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {5, 6},
                    {"/a", "/b"},
                    true));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftSemiJoinBatchOutput2) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 4, 5},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a"},
                    false));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {0, 1, 2, 3, 4},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b"},
                    false));

    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {6, 7, 8, 9},
                    {"/b", "/c", "/a", "/b"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {5},
                    {"/a"},
                    false));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {6},
                    {"/b"},
                    true));
}

TEST_F(LookupJoinKernelTest, testMultiFieldsSemiJoin) {
    prepareAttributeMultiFiledSemiJoin();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(
                        _allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(
                        _allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(
                        _allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(2, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(
                    outputTable, "path", {{"/a", "/a/b/c","/a/b"}, {"/a/b/c"}}));
}

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsSemiJoin) {
    prepareAttributeMultiFiledSemiJoin();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testMultiFieldsAntiJoin) {
    prepareAttributeMultiFiledSemiJoin();
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(1, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(
                    outputTable, "path", {{}}));
}

TEST_F(LookupJoinKernelTest, testEmptyMultiFieldsAntiJoin) {
    prepareAttributeMultiFiledSemiJoin();
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testSingleValJoinLeftAntiJoin) {
    prepareAttributeAntiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 3, 6, 10},
                    {"/a", "/a/b", "/a/c", "/b/c"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
            {10},
            {"/b/c"},
            true));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftAntiJoinBatchOutput) {
    prepareAttributeAntiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 10, 4, 5, 6, 7, 8, 9},
                    {"/a", "/a/b", "/a/c", "/b/c", "/c", "/a/b", "/a", "/b", "/c", "/a", "/b"},
                    true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {10, 7, 8, 9},
                    {"/c", "/c", "/a", "/b"},
                    true));
}

TEST_F(LookupJoinKernelTest, testSingleValIndexLeftAntiJoinBatch) {
    prepareAttributeAntiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 10, 4, 5, 6, 7, 8},
                    {"/a", "/a/b", "/a/c", "/b/c", "/c", "/a/b", "/a", "/b", "/c", "/a"},
                    false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {10, 7, 8},
                    {"/c", "/c", "/a"},
                    false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {9},
                    {"/b"},
                    true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {9},
                    {"/b"},
                    true));
}


TEST_F(LookupJoinKernelTest, testPartialIndexJoinLeftSemiJoin) {
    preparePartialIndexSemiJoin();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> rightDocs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "aid", {0, 3, 6, 10}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, rightDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, rightDocs, "group", {"groupA", "groupB", "groupA", "groupB"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

TEST_F(LookupJoinKernelTest, testNotEqualJoin) {
    prepareAttributeMultiFiledJoin();
    _attributeMap["is_equi_join"] = true;
    _attributeMap["remaining_condition"] =
        ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$attr", "$attr0"]})json");
    _attributeMap["equi_condition"] =
        ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");

    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
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

TEST_F(LookupJoinKernelTest, testGenStreamQuery) {
    TablePtr table = prepareTable();
    LookupJoinKernel kernel;
    Row row(0, 0);
    vector<string> termVec;
    map<string, IndexInfo> indexInfoMap;
    indexInfoMap["fid0"] = IndexInfo("fid0", "number");
    indexInfoMap["path0"] = IndexInfo("path0", "string");
    indexInfoMap["pathM0"] = IndexInfo("pathM0", "string");
    indexInfoMap["ids0"] = IndexInfo("ids0", "number");

    ASSERT_TRUE(kernel.genStreamQuery(table, row, string("xxxx"), string("fid0"), "fid0", "number") == nullptr);

    EXPECT_EQ("NumberQuery:[NumberTerm: [fid0,[1, 1||1|100|]]",
              kernel.genStreamQuery(table, row, string("fid"), string("fid0"), "fid0", "number")->toString());

    EXPECT_EQ("TermQuery:[Term:[path0||abc|100|]]",
              kernel.genStreamQuery(table, row, string("path"), string("path0"), "path0", "string")->toString());

    EXPECT_EQ("MultiTermQuery:[Term:[pathM0||/a|100|], Term:[pathM0||/a/b/c|100|], Term:[pathM0||/a/b|100|], ]",
              kernel.genStreamQuery(table, row, string("pathM"), string("pathM0"), "pathM0", "string")->toString());

    EXPECT_EQ("MultiTermQuery:[NumberTerm: [ids0,[1, 1||1|100|], NumberTerm: [ids0,[2, 2||2|100|], ]",
              kernel.genStreamQuery(table, row, string("ids"), string("ids0"), "ids0", "number")->toString());

    vector<string> fields = {"fid", "pathM"};
    vector<string> indexFields = {"fid0", "pathM0"};
    EXPECT_EQ("AndQuery:[NumberQuery:[NumberTerm: [fid0,[1, 1||1|100|]], MultiTermQuery:[Term:[pathM0||/a|100|], Term:[pathM0||/a/b/c|100|], Term:[pathM0||/a/b|100|], ], ]",
              kernel.genStreamQuery(table, row, fields, indexFields, indexInfoMap)->toString());

}

TEST_F(LookupJoinKernelTest, testGenStreamQueryTerm) {
    TablePtr table = prepareTable();
    LookupJoinKernel kernel;
    Row row(0, 0);
    vector<string> termVec;
    ASSERT_TRUE(kernel.genStreamQueryTerm(table, row, "fid", termVec));
    ASSERT_EQ(1, termVec.size());
    ASSERT_EQ("1", termVec[0]);

    termVec.clear();
    ASSERT_TRUE(kernel.genStreamQueryTerm(table, row, "pathM", termVec));
    ASSERT_EQ(3, termVec.size());
    ASSERT_EQ("/a", termVec[0]);
    ASSERT_EQ("/a/b/c", termVec[1]);
    ASSERT_EQ("/a/b", termVec[2]);

    termVec.clear();
    ASSERT_TRUE(kernel.genStreamQueryTerm(table, row, "path", termVec));
    ASSERT_EQ(1, termVec.size());
    ASSERT_EQ("abc", termVec[0]);

    termVec.clear();
    ASSERT_TRUE(kernel.genStreamQueryTerm(table, row, "ids", termVec));
    ASSERT_EQ(2, termVec.size());
    ASSERT_EQ("1", termVec[0]);
    ASSERT_EQ("2", termVec[1]);

    termVec.clear();
    ASSERT_FALSE(kernel.genStreamQueryTerm(table, row, "xxxx", termVec));

}

TEST_F(LookupJoinKernelTest, testLeftJoin) {
    prepareAttribute();
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"] =
        ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }

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
}

TEST_F(LookupJoinKernelTest, testLeftJoinSingleEmpty) {
    prepareAttributeMultiFiledJoin();
    _attributeMap["join_type"] = string("LEFT");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {"/a/b/c"};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(4, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(
                    outputTable, "path", {{"/a", "/a/b/c","/a/b"}, {"/a", "/a/b/c","/a/b"},
                                                                   {"/a/b/c"}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0",
                    {"/a", "/a/b", "/a/b/c", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", {0, 1, 3, 0}));
}

TEST_F(LookupJoinKernelTest, testLeftJoinAllEmpty) {
    prepareAttributeMultiFiledJoin();
    _attributeMap["join_type"] = string("LEFT");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        vector<string> subPaths = {};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);

        vector<string> subPaths1 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths1, &_pool);
        MultiString multiPath1;
        multiPath1.init(buf);

        vector<string> subPaths2 = {};
        buf = MultiValueCreator::createMultiStringBuffer(subPaths2, &_pool);
        MultiString multiPath2;
        multiPath2.init(buf);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath, multiPath1, multiPath2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "attr", {1, 2, 1}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }

    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);

    ASSERT_EQ(3, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn(
                    outputTable, "path", {{}, {}, {}}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0",
                    {"", "", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", {0, 0, 0}));
}

TEST_F(LookupJoinKernelTest, testLeftJoinBatchInput) {
    prepareAttribute();
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"] =
        ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
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
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {2, 0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a/b/c", "/a/c", "/a/c/c"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

TEST_F(LookupJoinKernelTest, testLeftJoinBatchOutput) {
    prepareAttribute();
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"] =
        ParseJson(R"json({"op":">", "type":"OTHER", "params":["$fid", 0]})json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
    }
    bool eof = false;
    TablePtr outputTable;
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(0, outputTable->getRowCount()) << TableUtil::toString(outputTable);

    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {2, 0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a/b/c", "/a/c", "/a/c/c"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(2, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {1, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b", "/a"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupA", ""}));


    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupB"}));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());


    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b/c"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/b/c"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupB"}));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {1, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/c/c", "/a/c"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a/c/c", ""}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupA", ""}));

    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());
}

TEST_F(LookupJoinKernelTest, testSemiJoinBatchInput) {
    prepareAttributeSemiJoinRightIsBuild();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {0, 1, 2, 3, 4, 5},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b", "/a"},
                    false));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {0, 1, 2, 3, 4},
                    {"/a", "/a/b", "/a/c", "/b/c", "/a/b"},
                    false));

    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {6, 7, 8, 9},
                    {"/b", "/c", "/a", "/b"},
                    false));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {5},
                    {"/a"},
                    false));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput(
                    {6},
                    {"/b"},
                    false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());
    ASSERT_TRUE(_testerPtr->setInputEof("input0"));
    ASSERT_NO_FATAL_FAILURE(checkOutputEof());
}

TEST_F(LookupJoinKernelTest, testEmptyJoinInner) {
    prepareAttribute();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {},
                    {},
                    true));
    ASSERT_NO_FATAL_FAILURE(checkInnerOutput({}, {}, {}, {}, {}, true));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinLeft) {
    prepareAttribute();
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {},
                    {},
                    true));
    ASSERT_NO_FATAL_FAILURE(checkInnerOutput({}, {}, {}, {}, {}, true));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinSemi) {
    prepareAttributeSemiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {},
                    {},
                    true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(LookupJoinKernelTest, testEmptyJoinAnti) {
    prepareAttributeAntiJoinRightIsBuild();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable(
                    {},
                    {},
                    true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(LookupJoinKernelTest, testPKJoin) {
    prepareAttributePKJoinOptimize();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "id", {1, 3}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_TRUE(eof);
    vector<string> expFid = {"0", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a/b", "/a/b/c"};
    checkOutput(expPath, odata, "path");
    vector<string> expAid = {"1", "3"};
    checkOutput(expAid, odata, "id");
    checkOutput(expAid, odata, "aid");
}

TEST_F(LookupJoinKernelTest, testPKJoinRightIsBuild) {
    prepareAttributePKJoinOptimizeRightIsBuild();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int64_t>(_allocator, docs, "id", {{1, 2, 7}, {6, 8}}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

TEST_F(LookupJoinKernelTest, testNumberJoinRightIsBuild) {
    prepareAttributeNumberJoinOptimizeRightIsBuild();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "fid", {22, 33}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "fattr", {2, 3}));
        auto rightTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", rightTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

class LookupJoinKernelStringPKTest : public LookupJoinKernelTest
{
public:
    void prepareInvertedTableData(string &tableName,
                                  string &fields, string &indexes, string &attributes,
                                  string &summarys, string &truncateProfileStr,
                                  string &docs, int64_t &ttl) override
    {
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
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$uuid":{"type":"primarykey64", "name":"uuid"}, "$name":{"type":"STRING", "name":"name"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$uid", "$cate_id", "$uuid", "$name"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$uuid", "$uid"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"item", "db_name":"default", "catalog_name":"default", "index_infos": {"$uuid":{"type":"primarykey64", "name":"uuid"}, "$name":{"type":"STRING", "name":"name"}}, "output_fields":["$uuid", "$name"], "use_nest_table":false, "table_type":"normal", "batch_size":1, "hash_fields":["$uuid"]})json");
    }
};

TEST_F(LookupJoinKernelStringPKTest, testPKJoin1) {
    prepareAttributeStringPKJoinOptimize();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
        vector<string> inputUids = {"a111", "d444", "h888"};
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "uid", inputUids));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "cate_id", {1, 4, 8}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

TEST_F(LookupJoinKernelStringPKTest, testPKJoin2) {
    prepareAttributeStringPKJoinOptimize();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 2));
        vector<vector<string>> inputUids = {{"b222", "c333"}, {"f666", "h888"}};
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator(_allocator, docs, "uid", inputUids));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "cate_id", {1, 4}));
        auto leftTable = createTable(_allocator, docs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
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

END_HA3_NAMESPACE(sql);
