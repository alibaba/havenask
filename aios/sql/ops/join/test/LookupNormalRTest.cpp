#include "sql/ops/join/LookupNormalR.h"

#include "ha3/common/CommonDef.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace isearch::common;

namespace sql {

class LookupNormalRTest : public OpTestBase {
public:
    LookupNormalRTest();
    ~LookupNormalRTest();

public:
    void setUp() override {}
    void tearDown() override {}

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
        return Table::fromMatchDocs(leftDocs, _allocator);
    }

public:
    MatchDocAllocatorPtr _allocator;
};

LookupNormalRTest::LookupNormalRTest()
    : _allocator(new MatchDocAllocator(_poolPtr.get())) {}

LookupNormalRTest::~LookupNormalRTest() {}

TEST_F(LookupNormalRTest, testGetDocidField_fieldSizeError) {
    string ret;
    ASSERT_FALSE(LookupNormalR::getDocIdField({}, {}, ret));
}

TEST_F(LookupNormalRTest, testGetDocidField_FieldNotInnerDocId) {
    string ret;
    ASSERT_FALSE(LookupNormalR::getDocIdField({"a"}, {"b"}, ret));
}

TEST_F(LookupNormalRTest, testGetDocidField) {
    string ret;
    ASSERT_TRUE(LookupNormalR::getDocIdField({"a"}, {string(INNER_DOCID_FIELD_NAME)}, ret));
    ASSERT_EQ("a", ret);

    LookupNormalR lookupNormalR;
    vector<string> leftFields, rightFields;
    leftFields = {INNER_DOCID_FIELD_NAME};
    rightFields = {"a"};
    lookupNormalR._leftTableIndexed = true;
    ASSERT_TRUE(lookupNormalR.getDocIdField(rightFields, leftFields, ret));
    ASSERT_EQ("a", ret);
    lookupNormalR._leftTableIndexed = false;
    ASSERT_FALSE(lookupNormalR.getDocIdField(leftFields, rightFields, ret));

    leftFields = {"a"};
    rightFields = {INNER_DOCID_FIELD_NAME};
    ASSERT_TRUE(lookupNormalR.getDocIdField(leftFields, rightFields, ret));
    ASSERT_EQ("a", ret);
}

TEST_F(LookupNormalRTest, testGenDocids_ColumnNotFound) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupNormalR::genDocIds({input, 2, 3}, "a", docIds));
    CHECK_TRACE_COUNT(1, "invalid join column name [a]", provider.getTrace(""));
}

TEST_F(LookupNormalRTest, testGenDocids_joinTypeNotSupport) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupNormalR::genDocIds({input, 2, 3}, "fid", docIds));
    CHECK_TRACE_COUNT(1, "inner docid join only support int32_t", provider.getTrace(""));
}

TEST_F(LookupNormalRTest, testGenDocids_joinTypeNotSupport2) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_FALSE(LookupNormalR::genDocIds({input, 2, 3}, "ids", docIds));
    CHECK_TRACE_COUNT(1, "inner docid join only support int32_t", provider.getTrace(""));
}

TEST_F(LookupNormalRTest, testGenDocids_Empty) {
    navi::NaviLoggerProvider provider("WARN");
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_TRUE(LookupNormalR::genDocIds({input, 1, 0}, "docid", docIds));
    ASSERT_TRUE(docIds.empty());
}

TEST_F(LookupNormalRTest, testGenDocids_Success) {
    navi::NaviLoggerProvider provider;
    TablePtr input = prepareTable();
    vector<docid_t> docIds;
    ASSERT_TRUE(LookupNormalR::genDocIds({input, 0, 1}, "docid", docIds));
    vector<docid_t> expected = {2};
    ASSERT_EQ(expected, docIds);
}

TEST_F(LookupNormalRTest, testSelectValidField) {
    TablePtr input = prepareTable();
    vector<string> inputFields;
    vector<string> joinFields;
    bool ret = LookupNormalR::selectValidField(input, inputFields, joinFields, false);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());

    vector<string> expectInput = {"fid", "path"};
    vector<string> expectJoin = {"a", "b", "c"};
    inputFields = expectInput;
    joinFields = expectJoin;
    ret = LookupNormalR::selectValidField(input, inputFields, joinFields, false);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());
    ASSERT_EQ(expectInput[0], inputFields[1]);
    ASSERT_EQ(expectInput[1], inputFields[0]);
    ASSERT_EQ(expectJoin[0], joinFields[1]);
    ASSERT_EQ(expectJoin[1], joinFields[0]);

    inputFields = expectInput;
    joinFields = expectJoin;
    ret = LookupNormalR::selectValidField(input, inputFields, joinFields, true);
    ASSERT_TRUE(ret);
    ASSERT_EQ(inputFields.size(), joinFields.size());
    ASSERT_EQ(expectInput[0], inputFields[0]);
    ASSERT_EQ(expectInput[1], inputFields[1]);
    ASSERT_EQ(expectJoin[0], joinFields[0]);
    ASSERT_EQ(expectJoin[1], joinFields[1]);
}

TEST_F(LookupNormalRTest, testDocIdsJoin) {
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
    auto leftData = createTable(_allocator, leftDocs);
    auto leftTable = getTable(leftData);
    vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
    auto rightData = createTable(_allocator, rightDocs);
    auto rightTable = getTable(rightData);
    {
        LookupNormalR lookupNormalR;
        JoinBaseParamR joinParamR;
        lookupNormalR._joinParamR = &joinParamR;
        ASSERT_TRUE(lookupNormalR.docIdsJoin({leftTable, 0, 2}, rightTable));
        ASSERT_EQ(2, lookupNormalR._joinParamR->_tableAIndexes.size());
        ASSERT_EQ(0, lookupNormalR._joinParamR->_tableAIndexes[0]);
        ASSERT_EQ(1, lookupNormalR._joinParamR->_tableAIndexes[1]);

        ASSERT_EQ(2, lookupNormalR._joinParamR->_tableBIndexes.size());
        ASSERT_EQ(0, lookupNormalR._joinParamR->_tableBIndexes[0]);
        ASSERT_EQ(1, lookupNormalR._joinParamR->_tableBIndexes[1]);
    }
    {
        LookupNormalR lookupNormalR;
        JoinBaseParamR joinParamR;
        lookupNormalR._joinParamR = &joinParamR;
        ASSERT_TRUE(lookupNormalR.docIdsJoin({leftTable, 1, 2}, rightTable));
        ASSERT_EQ(2, lookupNormalR._joinParamR->_tableAIndexes.size());
        ASSERT_EQ(1, lookupNormalR._joinParamR->_tableAIndexes[0]);
        ASSERT_EQ(2, lookupNormalR._joinParamR->_tableAIndexes[1]);

        ASSERT_EQ(2, lookupNormalR._joinParamR->_tableBIndexes.size());
        ASSERT_EQ(0, lookupNormalR._joinParamR->_tableBIndexes[0]);
        ASSERT_EQ(1, lookupNormalR._joinParamR->_tableBIndexes[1]);
    }
    {
        LookupNormalR lookupNormalR;
        JoinBaseParamR joinParamR;
        lookupNormalR._joinParamR = &joinParamR;
        ASSERT_FALSE(lookupNormalR.docIdsJoin({leftTable, 0, 3}, rightTable));
    }
    {
        LookupNormalR lookupNormalR;
        JoinBaseParamR joinParamR;
        lookupNormalR._joinParamR = &joinParamR;
        ASSERT_TRUE(lookupNormalR.docIdsJoin({leftTable, 2, 2}, rightTable));
    }
    {
        LookupNormalR lookupNormalR;
        JoinBaseParamR joinParamR;
        lookupNormalR._joinParamR = &joinParamR;
        ASSERT_FALSE(lookupNormalR.docIdsJoin({leftTable, 3, 1}, rightTable));
    }
}

} // namespace sql
