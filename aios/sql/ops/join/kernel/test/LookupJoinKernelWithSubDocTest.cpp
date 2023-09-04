#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
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
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "turing_ops_util/variant/Tracer.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class LookupJoinKernelWithSubDocTest : public OpTestBase {
public:
    LookupJoinKernelWithSubDocTest();
    ~LookupJoinKernelWithSubDocTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _attributeMap.clear();
    }

private:
    void prepareAttribute0() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$id"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$sub_id", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(
            R"json({"field_meta":[{"index_type":"primarykey64", "index_name":"aid","field_name":"$aid","field_type":"int32"}, {"index_type":"STRING", "index_name":"path", "field_name":"$path","field_type":"string"}, {"index_type":"primarykey64", "index_name":"sub_id", "field_name":"$sub_id","field_type":"int32"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path", "$id", "$aid", "$sub_id", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$sub_id"]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "table_meta": {"field_meta":[{"index_type":"primary_key","index_name":"aid","field_name":"$aid","field_type":"int32"},{"index_name":"sid","index_type":"secondary_key","field_name":"$sid","field_type":"int32"}]}, "output_fields_internal":["$aid", "$sub_id", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"output_fields":["sub_attr1","sub_id","sub_index2"],"output_fields_type":["INTEGER","BIGINT","VARCHAR"],"nest_field_counts":[3],"nest_field_types":["MULTISET"],"nest_field_names":["$acl"],"with_ordinality":false,"table_name":"acl"}], "condition":{"op": "AND", "params": [{"op":"=", "params":["$sub_attr1", 2]}, {"op":"=", "params":["$attr", 1]}]}, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttribute1() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$id"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$sub_id", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(
            R"json({"field_meta":[{"index_type":"primarykey64", "index_name":"aid","field_name":"$aid","field_type":"int32"}, {"index_type":"STRING", "index_name":"path", "field_name":"$path","field_type":"string"}, {"index_type":"primarykey64", "index_name":"sub_id", "field_name":"$sub_id","field_type":"int32"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path", "$id", "$aid", "$sub_id", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$aid"]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "table_meta": {"field_meta":[{"index_type":"primary_key","index_name":"aid","field_name":"$aid","field_type":"int32"},{"index_name":"sid","index_type":"secondary_key","field_name":"$sid","field_type":"int32"}]}, "output_fields_internal":["$aid", "$sub_id", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"table_name":"acl"}], "condition":{"op":"=", "params":["$sub_attr1", 1]}, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttribute2() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path", "$id"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$sub_id", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(
            R"json({"field_meta":[{"index_type":"primarykey64", "index_name":"aid","field_name":"$aid","field_type":"int32"}, {"index_type":"STRING", "index_name":"path", "field_name":"$path","field_type":"string"}, {"index_type":"primarykey64", "index_name":"sub_id", "field_name":"$sub_id","field_type":"int32"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$fid", "$path", "$id", "$aid", "$sub_id", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            // = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$id", "$sub_id"]})json");
            = ParseJson(
                R"json({"op": "AND", "params": [{"op":"=", "type":"OTHER", "params":["$id", "$sub_id"]}, {"op":"=", "type":"OTHER", "params":["$fid", "$aid"]}]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "table_meta": {"field_meta":[{"index_type":"primary_key","index_name":"aid","field_name":"$aid","field_type":"int32"},{"index_name":"sid","index_type":"secondary_key","field_name":"$sid","field_type":"int32"}]}, "output_fields_internal":["$aid", "$sub_id", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"output_fields":["sub_attr1","sub_id","sub_index2"],"output_fields_type":["INTEGER","BIGINT","VARCHAR"],"nest_field_counts":[3],"nest_field_types":["MULTISET"],"nest_field_names":["$acl"],"with_ordinality":false,"table_name":"acl"}], "condition":{"op": "OR", "params": [{"op":"=", "params":["$sub_attr1", 2]}, {"op":"=", "params":["$attr", 1]}]}, "table_type":"normal", "batch_size":1, "hash_fields":["$aid"]})json");
    }

    void prepareAttributeOutSub() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$sub_id", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(
            R"json({"field_meta":[{"index_type":"NUMBER", "index_name":"aid","field_name":"$aid","field_type":"int32"}, {"index_type":"STRING", "index_name":"path", "field_name":"$path","field_type":"string"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$fid", "$path", "$aid", "$sub_id", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "table_meta": {"field_meta":[{"index_type":"NUMBER","index_name":"aid","field_name":"$aid","field_type":"int32"}]}, "output_fields_internal":["$aid", "$sub_id", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"table_name":"acl"}], "condition":{"op":"=", "params":["$sub_attr1", 1]}, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeHasSub() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["right_table_meta"] = ParseJson(
            R"json({"field_meta":[{"index_type":"NUMBER", "index_name":"aid","field_name":"$aid","field_type":"int32"}, {"index_type":"STRING", "index_name":"path", "field_name":"$path","field_type":"string"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields_internal":["$aid", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"table_name":"acl"}], "condition":{"op":"=", "params":["$sub_attr1", 2]}, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

    void prepareInvertedTable() override {
        _tableName = "acl";
        string tableName = _tableName;
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
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
        MultiString multiPath;
        multiPath.init(buf);
        IF_FAILURE_RET_NULL(_matchDocUtil.extendMatchDocAllocator<MultiString>(
            _allocator, leftDocs, "pathM", {multiPath}));
        vector<int32_t> ids = {1, 2};
        buf = MultiValueCreator::createMultiValueBuffer(ids.data(), 2, _poolPtr.get());
        MultiInt32 multiIds;
        multiIds.init(buf);
        IF_FAILURE_RET_NULL(_matchDocUtil.extendMatchDocAllocator<MultiInt32>(
            _allocator, leftDocs, "ids", {multiIds}));
        TablePtr table(new Table(leftDocs, _allocator));
        return table;
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount);
        auto column = outputTable->getColumn(field);
        auto name = column->getColumnSchema()->getName();
        ASSERT_TRUE(column != NULL);
        for (size_t i = 0; i < rowCount; i++) {
            EXPECT_EQ(expect[i], column->toString(i)) << name << ":" << i;
        }
    }
    indexlib::partition::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
                                                              const std::string &tableName) {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
            tableName,
            "aid:int64;path:string;group:string;attr:int32",
            "aid:primarykey64:aid;path:string:path;attr:number:attr",
            "aid;path;group;attr",
            "aid;path;group;attr",
            ""); // truncateProfile

        auto subSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
            tableName,
            "sub_id:int64;sub_attr1:int32;sub_index2:string",           // fields
            "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", // indexs
            "sub_attr1;sub_id;sub_index2",                              // attributes
            "",                                                         // summarys
            "");                                                        // truncateProfile

        string docsStr
            = "cmd=add,aid=0,path=/"
              "a,attr=1,group=groupA,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
              "cmd=add,aid=1,path=/a/"
              "b,attr=1,group=groupA,sub_id=4^5^6,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
              "cmd=add,aid=2,path=/a/b,attr=2,group=groupB,sub_id=7,sub_attr1=1,sub_index2=aa;"
              "cmd=add,aid=3,path=/a/b/c,attr=2,group=groupB,sub_id=8,sub_attr1=1,sub_index2=aa;"
              "cmd=add,aid=4,path=/a/d/c,attr=3,group=groupA,sub_id=9,sub_attr1=1,sub_index2=ab;"
              "cmd=add,aid=5,path=/a/c/c,attr=1,group=groupA,sub_id=10,sub_attr1=1,sub_index2=ab;"
              "cmd=add,aid=6,path=/a/"
              "c,attr=2,group=groupB,sub_id=11^12,sub_attr1=2^1,sub_index2=abc^ab;";

        indexlib::config::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = indexlib::testlib::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema,
                                                                                       subSchema);

        indexlib::partition::IndexPartitionPtr indexPartition
            = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
                schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

public:
    MatchDocAllocatorPtr _allocator;
};

LookupJoinKernelWithSubDocTest::LookupJoinKernelWithSubDocTest() {}

LookupJoinKernelWithSubDocTest::~LookupJoinKernelWithSubDocTest() {}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSub) {
    prepareAttributeHasSub();
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
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_NO_FATAL_FAILURE(checkOutput({"0", "0", "1", "1"}, odata, "fid"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a", "/a/b", "/a/b"}, odata, "path"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a", "/a/b", "/a/b"}, odata, "path0"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"groupA", "groupA", "groupA", "groupA"}, odata, "group"));
}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSubAttr) {
    prepareAttributeOutSub();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {0, 1, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    ASSERT_NO_FATAL_FAILURE(checkOutput({"0", "1", "1"}, odata, "fid"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a/b", "/a/b"}, odata, "path"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a/b", "/a/b"}, odata, "path0"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"groupA", "groupA", "groupB"}, odata, "group"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"1", "4", "7"}, odata, "sub_id"));
}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSubAttrBatchSize1) {
    prepareAttributeOutSub();
    _attributeMap["batch_size"] = 1;
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {0, 1, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {0, 1, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a", "/a/b", "/a/b"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a", "/a/b", "/a/b"}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn(outputTable, "group", {"groupA", "groupA", "groupB"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {1, 4, 7}));
}

TEST_F(LookupJoinKernelWithSubDocTest, testJoinWithSubPK) {
    prepareAttribute0();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {3, 4, 5, 6}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist", "/a/c"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocator, leftDocs, "id", {1, 2, 4, 11}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(eof);
    ASSERT_TRUE(odata != nullptr);
    ASSERT_NO_FATAL_FAILURE(checkOutput({"4"}, odata, "fid"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a/b"}, odata, "path"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a"}, odata, "path0"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"groupA"}, odata, "group"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"2"}, odata, "sub_id"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"0"}, odata, "aid"));
}

TEST_F(LookupJoinKernelWithSubDocTest, testJoinWithSubPKLimitBatchSize) {
    prepareAttribute0();
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {3, 4, 5, 6}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist", "/a/c"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocator, leftDocs, "id", {1, 2, 4, 11}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }

    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a/b"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group", {"groupA"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "aid", {0}));
}

TEST_F(LookupJoinKernelWithSubDocTest, testJoinWithMainPK) {
    prepareAttribute1();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, leftDocs, "id", {0, 1, 2}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(eof);
    ASSERT_TRUE(odata != nullptr);
    ASSERT_NO_FATAL_FAILURE(checkOutput({"3", "4", "5"}, odata, "fid"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a/b", "/not_exist"}, odata, "path"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a/b", "/a/b"}, odata, "path0"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"groupA", "groupA", "groupB"}, odata, "group"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"1", "4", "7"}, odata, "sub_id"));
}

TEST_F(LookupJoinKernelWithSubDocTest, testJoinWithMainPKLimitBatchSize) {
    prepareAttribute1();
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["lookup_batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "fid", {3, 4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, leftDocs, "id", {0, 1, 2}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }

    TablePtr outputTable;
    ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));

    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "fid", {3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path", {"/a", "/a/b", "/not_exist"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "path0", {"/a", "/a/b", "/a/b"}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn(outputTable, "group", {"groupA", "groupA", "groupB"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {1, 4, 7}));
}

TEST_F(LookupJoinKernelWithSubDocTest, testJoinWithSubPKAndMainPK) {
    navi::NaviLoggerProvider provider("TRACE3");
    prepareAttribute2();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocator, leftDocs, "fid", {0, 0, 1, 2, 6}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "path", {"/a", "/b", "/a/b", "/a/c", "/c"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(
            _allocator, leftDocs, "id", {1, 2, 4, 7, 11}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(eof);
    ASSERT_TRUE(odata != nullptr);
    ASSERT_NO_FATAL_FAILURE(checkOutput({"0", "0", "1", "6"}, odata, "fid"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/b", "/a/b", "/c"}, odata, "path"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"/a", "/a", "/a/b", "/a/c"}, odata, "path0"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"groupA", "groupA", "groupA", "groupB"}, odata, "group"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"1", "2", "4", "11"}, odata, "sub_id"));
    ASSERT_NO_FATAL_FAILURE(checkOutput({"0", "0", "1", "6"}, odata, "aid"));
}

} // namespace sql
