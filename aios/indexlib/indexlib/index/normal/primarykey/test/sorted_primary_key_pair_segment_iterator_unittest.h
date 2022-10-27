#ifndef __INDEXLIB_SORTEDPRIMARYKEYPAIRSEGMENTITERATORTEST_H
#define __INDEXLIB_SORTEDPRIMARYKEYPAIRSEGMENTITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SortedPrimaryKeyPairSegmentIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SortedPrimaryKeyPairSegmentIteratorTest();
    ~SortedPrimaryKeyPairSegmentIteratorTest();

    DECLARE_CLASS_NAME(SortedPrimaryKeyPairSegmentIteratorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestIterator(file_system::FSOpenType fsOpenType,
                           const std::string& docStr, 
                           const std::string& expectResultStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyPairSegmentIteratorTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTEDPRIMARYKEYPAIRSEGMENTITERATORTEST_H
