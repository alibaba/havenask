#ifndef __INDEXLIB_ONDISKORDEREDPRIMARYKEYITERATORTEST_H
#define __INDEXLIB_ONDISKORDEREDPRIMARYKEYITERATORTEST_H

#include <autil/StringUtil.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/util/hash_string.h"

IE_NAMESPACE_BEGIN(index);

class OnDiskOrderedPrimaryKeyIteratorTest : public INDEXLIB_TESTBASE
{
public:
    OnDiskOrderedPrimaryKeyIteratorTest();
    ~OnDiskOrderedPrimaryKeyIteratorTest();

    DECLARE_CLASS_NAME(OnDiskOrderedPrimaryKeyIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestUint64SortedPkIterator();
    void TestUint128SortedPkIterator();
    void TestInitWithSortedPkSegmentDatas();

private:
    void DoTestUint64PkIterator(PrimaryKeyIndexType type);
    void DoTestUint128PkIterator(PrimaryKeyIndexType type);

    void DoTestInitWithSegmentDatas(PrimaryKeyIndexType type);

    template<typename Key>
    void CheckIterator(OnDiskOrderedPrimaryKeyIterator<Key>& iter,
                       std::string docsStr);
private:
    IE_LOG_DECLARE();
};


INDEXLIB_UNIT_TEST_CASE(OnDiskOrderedPrimaryKeyIteratorTest, TestUint64SortedPkIterator);
INDEXLIB_UNIT_TEST_CASE(OnDiskOrderedPrimaryKeyIteratorTest, TestUint128SortedPkIterator);
INDEXLIB_UNIT_TEST_CASE(OnDiskOrderedPrimaryKeyIteratorTest, TestInitWithSortedPkSegmentDatas);

//////////////////////////////////////////////////////////////

template<typename Key>
bool compFun(const std::pair<Key, docid_t>& left,
             const std::pair<Key, docid_t>& right)
{
    if (left.first == right.first)
    {
        return left.second < right.second;
    }
    return left.first < right.first;
}

template<typename Key>
void OnDiskOrderedPrimaryKeyIteratorTest::CheckIterator(
        OnDiskOrderedPrimaryKeyIterator<Key>& iter, std::string resultStr)
{
    std::vector<std::vector<std::string> > pairVector;
    autil::StringUtil::fromString(resultStr, pairVector, ",", ";");

    util::HashString hashFunc;
    std::vector<std::pair<Key, docid_t> > expectResults;
    for (size_t i = 0; i < pairVector.size(); i++)
    {
        Key hashKey;
        hashFunc.Hash(hashKey, pairVector[i][0].c_str(), pairVector[i][0].size());
        docid_t docid = INVALID_DOCID;
        ASSERT_TRUE(autil::StringUtil::fromString(pairVector[i][1], docid));
        expectResults.push_back(std::make_pair(hashKey, docid));
        docid++;
    }
    sort(expectResults.begin(), expectResults.end(), compFun<Key>);
    size_t cursor = 0;
    while (iter.HasNext())
    {
        typename OnDiskOrderedPrimaryKeyIterator<Key>::PKPair pkPair;
        iter.Next(pkPair);
        EXPECT_EQ(expectResults[cursor].first, pkPair.key) << cursor;
        EXPECT_EQ(expectResults[cursor].second, pkPair.docid) << cursor;
        cursor++;
    }

    ASSERT_EQ(cursor, expectResults.size());
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ONDISKORDEREDPRIMARYKEYITERATORTEST_H
