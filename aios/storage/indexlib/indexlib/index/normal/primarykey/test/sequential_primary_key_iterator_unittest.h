#ifndef __INDEXLIB_SEQUENTIALPRIMARYKEYITERATORTEST_H
#define __INDEXLIB_SEQUENTIALPRIMARYKEYITERATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/sequential_primary_key_iterator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/HashString.h"

namespace indexlib { namespace index {

class SequentialPrimaryKeyIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SequentialPrimaryKeyIteratorTest();
    ~SequentialPrimaryKeyIteratorTest();

    DECLARE_CLASS_NAME(SequentialPrimaryKeyIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestIteratorSortArray();
    void TestIteratorBlockArray();

private:
    void InnerTestIterator(PrimaryKeyIndexType pkIndexType, const std::string& docStr,
                           const std::string& expectResults);

    template <typename Key>
    void CheckIterator(SequentialPrimaryKeyIterator<Key>& iter, bool sortKey, const std::vector<size_t>& pkCountVec,
                       const std::string& resultStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SequentialPrimaryKeyIteratorTest, TestIteratorSortArray);
INDEXLIB_UNIT_TEST_CASE(SequentialPrimaryKeyIteratorTest, TestIteratorBlockArray);

/////////////////////////////////////////////////////////////////////
template <typename Key>
bool CompPkPair(const std::pair<Key, docid_t>& left, const std::pair<Key, docid_t>& right)
{
    if (left.first == right.first) {
        return left.second < right.second;
    }
    return left.first < right.first;
}

template <typename Key>
void SequentialPrimaryKeyIteratorTest::CheckIterator(SequentialPrimaryKeyIterator<Key>& iter, bool sortKey,
                                                     const std::vector<size_t>& pkCountVec,
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
        size_t cursor = 0;
        for (size_t i = 0; i < pkCountVec.size(); ++i) {
            sort(expectResults.begin() + cursor, expectResults.begin() + cursor + pkCountVec[i], CompPkPair<Key>);
            cursor += pkCountVec[i];
        }
    }

    size_t cursor = 0;
    while (iter.HasNext()) {
        PKPair<Key> pkPair;
        iter.Next(pkPair);
        EXPECT_EQ(expectResults[cursor].first, pkPair.key) << cursor;
        EXPECT_EQ(expectResults[cursor].second, pkPair.docid) << cursor;
        cursor++;
    }
    ASSERT_EQ(cursor, expectResults.size());
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SEQUENTIALPRIMARYKEYITERATORTEST_H
