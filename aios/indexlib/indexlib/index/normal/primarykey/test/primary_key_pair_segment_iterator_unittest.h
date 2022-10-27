#ifndef __INDEXLIB_PRIMARYKEYPAIRSEGMENTITERATORTEST_H
#define __INDEXLIB_PRIMARYKEYPAIRSEGMENTITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_pair_segment_iterator.h"
#include "indexlib/util/hash_string.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyPairSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyPairSegmentIteratorTest();
    ~PrimaryKeyPairSegmentIteratorTest();

    DECLARE_CLASS_NAME(PrimaryKeyPairSegmentIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSortedPKPairIterator();

private:
    typedef std::tr1::shared_ptr<PrimaryKeyPairSegmentIterator<uint64_t> > PrimaryKeyPairSegmentIteratorPtr;

private:
    template<typename Key>
    void CheckIterator(const PrimaryKeyPairSegmentIteratorPtr& iter,
                       std::string resultStr);
private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////
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
void PrimaryKeyPairSegmentIteratorTest::CheckIterator(
        const PrimaryKeyPairSegmentIteratorPtr& iter, std::string resultStr)
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

    typename PrimaryKeyFormatter<Key>::PKPair pkPair;
    for (size_t cursor = 0; cursor < expectResults.size(); ++cursor)
    {
        iter->GetCurrentPKPair(pkPair);
        EXPECT_EQ(expectResults[cursor].first, pkPair.key) << cursor;
        EXPECT_EQ(expectResults[cursor].second, pkPair.docid) << cursor;
        if (cursor != expectResults.size() - 1)
        {
            ASSERT_TRUE(iter->HasNext());
            iter->Next(pkPair);
            EXPECT_EQ(expectResults[cursor].first, pkPair.key) << cursor;
            EXPECT_EQ(expectResults[cursor].second, pkPair.docid) << cursor;
        }
    } 
}

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPairSegmentIteratorTest, TestSortedPKPairIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARYKEYPAIRSEGMENTITERATORTEST_H
