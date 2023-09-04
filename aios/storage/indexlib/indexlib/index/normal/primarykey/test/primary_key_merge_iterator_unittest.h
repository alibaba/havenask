#ifndef __INDEXLIB_PRIMARYKEYMERGEITERATORTEST_H
#define __INDEXLIB_PRIMARYKEYMERGEITERATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_merge_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/HashString.h"

namespace indexlib { namespace index {

class PrimaryKeyMergeIteratorTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyMergeIteratorTest();
    ~PrimaryKeyMergeIteratorTest();

    DECLARE_CLASS_NAME(PrimaryKeyMergeIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestIterate();

private:
    void InitReclaimMap(const ReclaimMapPtr& reclaimMap, uint32_t docCount, const std::string& mapStr = "");

    void InnerTestIterator(const std::string& docStr, const std::string& reclaimMapStr,
                           const std::string& expectResults);

    void InnerTestIteratorOnPkIndexType(PrimaryKeyIndexType pkIndexType, const std::string& docStr,
                                        const std::string& reclaimMapStr, const std::string& expectResults);

    template <typename Key>
    void CheckIterator(PrimaryKeyMergeIterator<Key>& iter, bool sortKey, const std::string& resultStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyMergeIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyMergeIteratorTest, TestIterate);

///////////////////////////////////////////////////////////////////////
template <typename Key>
bool compPkPair(const std::pair<Key, docid_t>& left, const std::pair<Key, docid_t>& right)
{
    if (left.first == right.first) {
        return left.second < right.second;
    }
    return left.first < right.first;
}

template <typename Key>
void PrimaryKeyMergeIteratorTest::CheckIterator(PrimaryKeyMergeIterator<Key>& iter, bool sortKey,
                                                const std::string& resultStr)
{
    std::vector<std::vector<std::string>> pairVector;
    autil::StringUtil::fromString(resultStr, pairVector, ":", ",");

    util::HashString hashFunc;
    std::vector<std::pair<Key, docid_t>> expectResults;
    for (size_t i = 0; i < pairVector.size(); i++) {
        Key hashKey;
        hashFunc.Hash(hashKey, pairVector[i][0].c_str(), pairVector[i][0].size());
        docid_t docid = INVALID_DOCID;
        ASSERT_TRUE(autil::StringUtil::fromString(pairVector[i][1], docid));
        expectResults.push_back(std::make_pair(hashKey, docid));
    }
    if (sortKey) {
        sort(expectResults.begin(), expectResults.end(), compPkPair<Key>);
    }

    size_t cursor = 0;
    while (iter.HasNext()) {
        typename PrimaryKeyMergeIterator<Key>::TypedPKPair pkPair;
        iter.Next(pkPair);
        EXPECT_EQ(expectResults[cursor].first, pkPair.key) << cursor;
        EXPECT_EQ(expectResults[cursor].second, pkPair.docid) << cursor;
        cursor++;
    }
    ASSERT_EQ(cursor, expectResults.size());
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARYKEYMERGEITERATORTEST_H
