#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/agg/AggBase.h>
#include <ha3/sql/ops/agg/AggLocal.h>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class AggBaseTest : public OpTestBase {
public:
    AggBaseTest();
    ~AggBaseTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
private:
    MatchDocAllocatorPtr _allocator;
};

AggBaseTest::AggBaseTest() {
}

AggBaseTest::~AggBaseTest() {
}

TEST_F(AggBaseTest, testCalcHash) {
    navi::NaviLoggerProvider provider;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 2, 3}));
    auto table = getTable(createTable(_allocator, docs));
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(AggBase::calculateGroupKeyHash(table, groupKeyVec, result));
    ASSERT_EQ("", provider.getErrorMessage());
}

TEST_F(AggBaseTest, testCalcHashGroupNotFind) {
    navi::NaviLoggerProvider provider;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 2, 3}));
    auto table = getTable(createTable(_allocator, docs));
    vector<string> groupKeyVec = {"b"};
    vector<size_t> result;
    ASSERT_FALSE(AggBase::calculateGroupKeyHash(table, groupKeyVec, result));
    ASSERT_EQ("invalid column name [b]", provider.getErrorMessage());
}

TEST_F(AggBaseTest, testCalcHashMultiType) {
    navi::NaviLoggerProvider provider;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 3));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "a", {{1}, {2}, {3}}));
    auto table = getTable(createTable(_allocator, docs));
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(AggBase::calculateGroupKeyHash(table, groupKeyVec, result));
    ASSERT_EQ("", provider.getErrorMessage());
    ColumnPtr column = table->getColumn("a");
    ASSERT_NE(nullptr, column);
    auto *columnData = column->getColumnData<MultiInt32>();
    ASSERT_NE(nullptr, columnData);
    ASSERT_EQ(3, result.size());
    ASSERT_EQ(TableUtil::calculateHashValue(columnData->get(0)), result[0]);
    ASSERT_EQ(TableUtil::calculateHashValue(columnData->get(1)), result[1]);
    ASSERT_EQ(TableUtil::calculateHashValue(columnData->get(2)), result[2]);
}

TEST_F(AggBaseTest, testCalcHashMultiChar) {
    navi::NaviLoggerProvider provider;
    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 1));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "a", {"hello"}));
    auto table = getTable(createTable(_allocator, docs));
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(AggBase::calculateGroupKeyHash(table, groupKeyVec, result));
    ASSERT_EQ("", provider.getErrorMessage());
}


END_HA3_NAMESPACE(sql);
