#include "sql/ops/remoteScan/TableServiceConnector.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "multi_call/common/ErrorCode.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace table;
using namespace multi_call;

namespace sql {

class TableServiceConnectorTest : public TESTBASE {
public:
    TableServiceConnectorTest() {}
    ~TableServiceConnectorTest() {}
};

TEST_F(TableServiceConnectorTest, testGroupPksByPart) {
    TableServiceConnector connector;
    auto result = connector.groupByPartition({"1", "2", "3"}, "HASH64", 256);
    autil::HashFunctionBasePtr func = autil::HashFuncFactory::createHashFunc("HASH64");
    ASSERT_EQ(3, result.size());
    for (const auto &iter : result) {
        for (const auto &pk : iter.second) {
            ASSERT_EQ(iter.first, func->getHashId(pk.c_str()) / 256);
        }
    }
}

TEST_F(TableServiceConnectorTest, testMergeTableResult) {
    TableServiceConnector connector;
    MatchDocUtil matchDocUtil;
    auto pool = std::make_shared<autil::mem_pool::Pool>();
    auto allocator = std::make_shared<matchdoc::MatchDocAllocator>(pool);

    const auto &docs = matchDocUtil.createMatchDocs(allocator, 3);
    ASSERT_NO_FATAL_FAILURE(matchDocUtil.extendMatchDocAllocatorWithConstructFlag<int32_t>(
        allocator, docs, "int_field", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(matchDocUtil.extendMatchDocAllocatorWithConstructFlag<string>(
        allocator, docs, "str_field", {"str1", "str2", "str3"}));
    ASSERT_NO_FATAL_FAILURE(matchDocUtil.extendMatchDocAllocatorWithConstructFlag<double>(
        allocator, docs, "double_field", {1.0, 2.0, 3.0}));
    ASSERT_NO_FATAL_FAILURE(
        matchDocUtil.extendMultiValueMatchDocAllocatorWithConstructFlag<int32_t>(
            allocator, docs, "multi_int_field", {{1}, {2, 3}, {4, 5, 6}}));
    ASSERT_NO_FATAL_FAILURE(matchDocUtil.extendMultiValueMatchDocAllocatorWithConstructFlag<double>(
        allocator, docs, "multi_double_field", {{1.0}, {2.0, 3.0}, {4.0, 5.0, 6.0}}));
    for (size_t i = 0; i < 10; i++) {
        std::string fieldName = std::string("multi_str_field_") + autil::StringUtil::toString(i);
        ASSERT_NO_FATAL_FAILURE(
            matchDocUtil.extendMultiValueMatchDocAllocatorWithConstructFlag<string>(
                allocator, docs, fieldName, {{}, {"str1", "str2"}, {"str3", "str4", "str5"}}));
    }

    string result;
    TablePtr table1 = std::make_shared<table::Table>(docs, allocator);
    table1->serializeToString(result, pool.get());
    TablePtr table;
    ASSERT_TRUE(connector.mergeTableResult(table, result, pool));

    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ(15, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn<int32_t>(table, "int_field", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn<double>(table, "double_field", {1.0, 2.0, 3.0}));
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputColumn<string>(table, "str_field", {"str1", "str2", "str3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<int32_t>(
        table, "multi_int_field", {{1}, {2, 3}, {4, 5, 6}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<double>(
        table, "multi_double_field", {{1.0}, {2.0, 3.0}, {4.0, 5.0, 6.0}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<string>(
        table, "multi_str_field_0", {{}, {"str1", "str2"}, {"str3", "str4", "str5"}}));

    ASSERT_TRUE(connector.mergeTableResult(table, result, pool));
    ASSERT_EQ(6, table->getRowCount());
    ASSERT_EQ(15, table->getColumnCount());
}

} // namespace sql
