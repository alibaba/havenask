#include "sql/ops/join/LookupR.h"

#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Table.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;

namespace sql {

class LookupRTest : public OpTestBase {
public:
    LookupRTest();
    ~LookupRTest();

private:
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

LookupRTest::LookupRTest()
    : _allocator(new MatchDocAllocator(_poolPtr.get())) {}

LookupRTest::~LookupRTest() {}

TEST_F(LookupRTest, testGenStreamQueryTerm) {
    TablePtr input = prepareTable();
    vector<string> termVec;
    Row row;
    row.docid = 0;
    row.index = 0;
    string field = "fid";
    bool ret = LookupR::genStreamQueryTerm(input, row, field, termVec);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(termVec.empty());
    ASSERT_EQ("1", termVec[0]);
    termVec.clear();

    string mfield = "ids";
    ret = LookupR::genStreamQueryTerm(input, row, mfield, termVec);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(termVec.empty());
    ASSERT_EQ("1", termVec[0]);
    ASSERT_EQ("2", termVec[1]);
    termVec.clear();
}

TEST_F(LookupRTest, testGetPkTriggerField) {
    vector<string> inputFields = {"t1", "t2", "t3"};
    vector<string> joinFields = {"f1", "f2", "f3", "f4"};
    map<string, sql::IndexInfo> indexInfoMap;
    string expectField = "null";
    string triggerField = expectField;
    indexInfoMap["pk1"] = sql::IndexInfo("f1", "long");
    indexInfoMap["pk2"] = sql::IndexInfo("ff2", "primary_key");
    indexInfoMap["pk3"] = sql::IndexInfo("f4", "primarykey128");
    bool ret = LookupR::getPkTriggerField(inputFields, joinFields, indexInfoMap, triggerField);
    ASSERT_FALSE(ret);
    ASSERT_EQ(expectField, triggerField);

    indexInfoMap["pk2"] = sql::IndexInfo("f2", "primarykey64");
    ret = LookupR::getPkTriggerField(inputFields, joinFields, indexInfoMap, triggerField);
    ASSERT_TRUE(ret);
    ASSERT_NE(expectField, triggerField);
    ASSERT_EQ("t2", triggerField);
}

} // namespace sql
