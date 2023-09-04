#include "sql/ops/runSqlGraph/PartAccessAssigner.h"

#include <algorithm>
#include <cstdint>
#include <google/protobuf/stubs/common.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "autil/HashFunctionBase.h"
#include "autil/RangeUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/resource/GigClientR.h"
#include "sql/common/TableDistribution.h"
#include "sql/resource/HashFunctionCacheR.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;
using namespace autil;

namespace sql {

class MockSearchServiceForPartAccess : public multi_call::SearchService {
public:
    int32_t getBizPartCount(const std::string &bizName) const override {
        auto it = _bizPartCountMap.find(bizName);
        if (it == _bizPartCountMap.end()) {
            return 0;
        }
        return it->second;
    }

public:
    std::map<std::string, int32_t> _bizPartCountMap;
};

class PartAccessAssignerTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    GigClientR _gigClientR;
};

void PartAccessAssignerTest::setUp() {
    auto *searchService = new MockSearchServiceForPartAccess();
    searchService->_bizPartCountMap.emplace("searcher_biz", 8);
    _gigClientR._searchService.reset(searchService);
}

void PartAccessAssignerTest::tearDown() {}

TEST_F(PartAccessAssignerTest, testProcess_Success_NoSubGraph) {
    GraphDef def;
    GraphBuilder builder(&def);
    ASSERT_TRUE(builder.ok());

    PartAccessAssigner assigner(nullptr);
    GraphDef copyDef = def;
    NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(assigner.process(&copyDef));

    ASSERT_EQ(def.DebugString(), copyDef.DebugString());
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "[0] sub graphs processed", provider));
}

TEST_F(PartAccessAssignerTest, testProcess_Success_OneSubGraph) {
    GraphDef def;
    GraphBuilder builder(&def);
    builder.newSubGraph("main_biz");
    ASSERT_TRUE(builder.ok());

    PartAccessAssigner assigner(nullptr);
    GraphDef copyDef = def;
    NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(assigner.process(&copyDef));

    ASSERT_EQ(def.DebugString(), copyDef.DebugString());
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "[0] sub graphs processed", provider));
}

TEST_F(PartAccessAssignerTest, testProcess_Error_processSubGraph) {
    GraphDef def;
    GraphBuilder builder(&def);
    builder.newSubGraph("main_biz");
    builder.newSubGraph("searcher_biz");
    builder.subGraphAttr("table_distribution", "{,,,}");

    PartAccessAssigner assigner(nullptr);
    NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(assigner.process(&def));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "process sub graph [1] failed", provider));
}

TEST_F(PartAccessAssignerTest, testProcess_Success_Full) {
    GraphDef def;
    GraphBuilder builder(&def);
    builder.newSubGraph("main_biz");
    builder.newSubGraph("searcher_biz");
    builder.subGraphAttr("table_distribution",
                         R"json({"assigned_partition_ids": "1,2,3", "partition_cnt":64})json");
    ASSERT_TRUE(builder.ok());
    ASSERT_EQ(2, def.sub_graphs_size());

    PartAccessAssigner assigner(&_gigClientR);
    NaviLoggerProvider provider("TRACE3");
    auto copyDef = def;
    ASSERT_TRUE(assigner.process(&copyDef));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "[1] sub graphs processed", provider));

    ASSERT_EQ(2, copyDef.sub_graphs_size());
    ASSERT_EQ(def.sub_graphs(0).DebugString(), copyDef.sub_graphs(0).DebugString());

    auto &searcherSubGraph = copyDef.sub_graphs(1);
    auto &partInfoDef = searcherSubGraph.location().part_info();
    ASSERT_EQ(3, partInfoDef.part_ids_size());
    ASSERT_EQ(1, partInfoDef.part_ids(0));
    ASSERT_EQ(2, partInfoDef.part_ids(1));
    ASSERT_EQ(3, partInfoDef.part_ids(2));
    ASSERT_EQ(0, searcherSubGraph.binary_attrs().count("table_distribution"))
        << "table_distribution field should be erased";
}

TEST_F(PartAccessAssignerTest, testProcessSubGraph_Success_NoTableDistribution) {
    SubGraphDef subGraphDef;
    SubGraphDef copySubGraphDef = subGraphDef;

    PartAccessAssigner assigner(nullptr);
    NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(assigner.processSubGraph(&copySubGraphDef));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "table distribution not found, skip", provider));
    ASSERT_EQ(subGraphDef.DebugString(), copySubGraphDef.DebugString());
}

TEST_F(PartAccessAssignerTest, testProcessSubGraph_Error_ParseTableDistributionFailed) {
    SubGraphDef subGraphDef;
    GraphBuilder builder(&subGraphDef);
    builder.subGraphAttr("table_distribution", "{,,,}"); // key reason

    PartAccessAssigner assigner(nullptr);
    NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(assigner.processSubGraph(&subGraphDef));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "parse table distribution json failed", provider));
}

TEST_F(PartAccessAssignerTest, testProcessSubGraph_Error_ConvertTableDistToPartIdsFailed) {
    SubGraphDef subGraphDef;
    subGraphDef.mutable_location()->set_biz_name("searcher_biz");
    GraphBuilder builder(&subGraphDef);
    TableDistribution tableDist;
    tableDist.debugPartIds = "1,2";
    builder.subGraphAttr("table_distribution", FastToJsonString(tableDist).c_str());

    PartAccessAssigner assigner(nullptr); // key reason
    NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(assigner.processSubGraph(&subGraphDef));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "convert table dist to part ids failed", provider));
}

TEST_F(PartAccessAssignerTest, testProcessSubGraph_Success_Full) {
    SubGraphDef subGraphDef;
    subGraphDef.mutable_location()->set_biz_name("searcher_biz");
    GraphBuilder builder(&subGraphDef);
    TableDistribution tableDist;
    tableDist.debugPartIds = "1,2";
    builder.subGraphAttr("table_distribution", FastToJsonString(tableDist).c_str());

    PartAccessAssigner assigner(&_gigClientR);
    ASSERT_TRUE(assigner.processSubGraph(&subGraphDef));
    auto &partInfoDef = subGraphDef.location().part_info();
    ASSERT_EQ(2, partInfoDef.part_ids_size());
    ASSERT_EQ(1, partInfoDef.part_ids(0));
    ASSERT_EQ(2, partInfoDef.part_ids(1));
    ASSERT_EQ(0, subGraphDef.binary_attrs().count("table_distribution"));
}

TEST_F(PartAccessAssignerTest, testConvertTableDistToPartIds_Error_NoGigClientR) {
    TableDistribution tableDist;
    SubGraphDef subGraphDef;
    GraphBuilder builder(&subGraphDef);

    PartAccessAssigner assigner(nullptr);
    NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(assigner.convertTableDistToPartIds("searcher_biz", tableDist, builder));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "gig client resource is null", provider));
}

TEST_F(PartAccessAssignerTest, testConvertTableDistToPartIds_Error_GetFullPartCount) {
    TableDistribution tableDist;
    SubGraphDef subGraphDef;
    GraphBuilder builder(&subGraphDef);

    PartAccessAssigner assigner(&_gigClientR);
    NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(assigner.convertTableDistToPartIds("not_existed_biz", tableDist, builder));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "get biz full part count failed", provider));
}

TEST_F(PartAccessAssignerTest, testConvertTableDistToPartIds_Success_Full) {
    TableDistribution tableDist;
    tableDist.debugPartIds = "1,2, 3";

    SubGraphDef subGraphDef;
    GraphBuilder builder(&subGraphDef);

    PartAccessAssigner assigner(&_gigClientR);
    ASSERT_TRUE(assigner.convertTableDistToPartIds("searcher_biz", tableDist, builder));

    auto &partInfoDef = subGraphDef.location().part_info();
    ASSERT_EQ(3, partInfoDef.part_ids_size());
    ASSERT_EQ(1, partInfoDef.part_ids(0));
    ASSERT_EQ(2, partInfoDef.part_ids(1));
    ASSERT_EQ(3, partInfoDef.part_ids(2));
}

TEST_F(PartAccessAssignerTest, genAccessPart) {
    PartAccessAssigner partAccessAssigner(nullptr);
    { // access all part
        TableDistribution tableDist;
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // access one part
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <= 3);
    }
    { // access one part
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        tableDist.hashMode._hashFields.push_back("metric");
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <= 3);
    }

    { // func name is empty
        TableDistribution tableDist;
        tableDist.hashValues["metric"].push_back("sys.cpu");
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // hash values not found
        TableDistribution tableDist;
        tableDist.hashValues["metric1"].push_back("1");
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // hash func error
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH_ERROR";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    {
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 3));
    }
    { // with hash value sep
        TableDistribution tableDist;
        tableDist.hashValues["metric"] = {"0|65535", "40000"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashValuesSep["metric"] = "|";
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(3, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 2, 3));
    }
    { // pk with broadcast table
        TableDistribution tableDist;
        tableDist.partCnt = 1;
        tableDist.hashValues["metric"] = {"0", "65535"};
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_TRUE(*partIds.begin() >= 0 && *partIds.begin() <= 3);
    }
}

TEST_F(PartAccessAssignerTest, getPartId) {
    { // hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 99};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    { // hashId first less than ranges,hashId second in ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {1, 150};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    { // hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {100, 200};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    { // hashId first greater than ranges,hashId second less than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {120, 120};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    { // hashId first in ranges,hashId second greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {99, 300};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    { // hashId first greater than ranges
        RangeVec ranges = {{100, 200}};
        PartitionRange hashId = {201, 300};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(0, partIds.size());
    }
    { // two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {200, 400};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
    { // two ranges normal
        RangeVec ranges = {{100, 200}, {400, 600}};
        PartitionRange hashId = {201, 420};
        set<int32_t> partIds;
        PartAccessAssigner::getPartId(ranges, hashId, partIds);
        ASSERT_EQ(1, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(1));
    }
}

TEST_F(PartAccessAssignerTest, getPartIds) {
    { // no hashId query
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 99}, {300, 399}};
        set<int32_t> partIds = PartAccessAssigner::getPartIds(ranges, hashIds);
        ASSERT_EQ(0, partIds.size());
    }
    { // normal hashId
        RangeVec ranges = {{100, 200}, {400, 600}};
        RangeVec hashIds = {{0, 300}, {301, 620}};
        set<int32_t> partIds = PartAccessAssigner::getPartIds(ranges, hashIds);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
}

TEST_F(PartAccessAssignerTest, createHashFunc) {
    PartAccessAssigner partAccessAssigner(nullptr);
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH";
        hashMode._hashFields.push_back("test");
        auto hashFunc = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH_not_exist";
        hashMode._hashFields.push_back("test");
        auto hashFunc = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc == nullptr);
    }
}

TEST_F(PartAccessAssignerTest, createHashFuncWithCache) {
    HashFunctionCacheR hashFunctionR;
    PartAccessAssigner partAccessAssigner(nullptr, &hashFunctionR);
    autil::HashFunctionBasePtr hashFunc1;
    autil::HashFunctionBasePtr hashFunc2;
    autil::HashFunctionBasePtr hashFunc3;
    autil::HashFunctionBasePtr hashFunc4;
    autil::HashFunctionBasePtr hashFunc5;
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH";
        hashMode._hashFields.push_back("test");
        hashFunc1 = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc1 != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "HASH";
        hashMode._hashFields.push_back("test");
        hashFunc2 = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc2 != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "ROUTING_HASH";
        hashMode._hashParams["aa"] = "bb";
        hashMode._hashFields.push_back("test");
        hashFunc3 = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc3 != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "ROUTING_HASH";
        hashMode._hashParams["aa"] = "bb";
        hashMode._hashFields.push_back("test");
        hashFunc4 = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc4 != nullptr);
    }
    {
        HashMode hashMode;
        hashMode._hashFunction = "ROUTING_HASH";
        hashMode._hashParams["aa"] = "bb";
        hashMode._hashParams["aaa"] = "bbb";
        hashMode._hashFields.push_back("test");
        hashFunc5 = partAccessAssigner.createHashFunc(hashMode);
        ASSERT_TRUE(hashFunc5 != nullptr);
    }
    ASSERT_TRUE(hashFunc1.get() != hashFunc2.get());
    ASSERT_EQ(hashFunc3.get(), hashFunc4.get());
    ASSERT_TRUE(hashFunc2.get() != hashFunc3.get());
    ASSERT_TRUE(hashFunc4.get() != hashFunc5.get());
    ASSERT_EQ(2, hashFunctionR._hashFuncMap.size());
}

TEST_F(PartAccessAssignerTest, genAllPart) {
    {
        set<int32_t> partIds = PartAccessAssigner::genAllPart(1);
        ASSERT_EQ(1, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0));
    }
    {
        set<int32_t> partIds = PartAccessAssigner::genAllPart(2);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1));
    }
    {
        set<int32_t> partIds = PartAccessAssigner::genAllPart(0);
        ASSERT_EQ(0, partIds.size());
    }
    {
        set<int32_t> partIds = PartAccessAssigner::genAllPart(-1);
        ASSERT_EQ(0, partIds.size());
    }
}

TEST_F(PartAccessAssignerTest, genDebugAccessPart) {
    PartAccessAssigner partAccessAssigner(nullptr);
    { // access all part
        TableDistribution tableDist;
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // access part 3
        TableDistribution tableDist;
        tableDist.debugPartIds = "3";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(3, *partIds.begin());
    }
    { // access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "4";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "-1";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // access part out range
        TableDistribution tableDist;
        tableDist.debugPartIds = "abc";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(4, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 1, 2, 3));
    }
    { // access part out range
        TableDistribution tableDist;
        tableDist.debugHashValues = "1,60000";
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(0, 3));
    }
    { // access part out range
        TableDistribution tableDist;
        tableDist.debugHashValues = "1";
        tableDist.debugPartIds = "2,3";
        tableDist.hashMode._hashFields.push_back("metric");
        tableDist.hashMode._hashFunction = "NUMBER_HASH";
        set<int32_t> partIds = partAccessAssigner.genAccessPart(4, tableDist);
        ASSERT_EQ(2, partIds.size());
        ASSERT_THAT(partIds, testing::ElementsAre(2, 3));
    }
}

} // namespace sql
