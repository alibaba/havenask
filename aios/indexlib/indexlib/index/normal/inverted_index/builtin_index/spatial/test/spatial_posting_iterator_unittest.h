#ifndef __INDEXLIB_SPATIALPOSTINGITERATORTEST_H
#define __INDEXLIB_SPATIALPOSTINGITERATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SpatialPostingIteratorTest : public INDEXLIB_TESTBASE
{
public:
    SpatialPostingIteratorTest();
    ~SpatialPostingIteratorTest();

    DECLARE_CLASS_NAME(SpatialPostingIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMember();
    void TestClone();
    void TestUnpack();
    void TestTermMeta();
    void TestReset();
    void TestSeekDocCount();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestMember);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestUnpack);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestTermMeta);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestReset);
INDEXLIB_UNIT_TEST_CASE(SpatialPostingIteratorTest, TestSeekDocCount);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIALPOSTINGITERATORTEST_H
