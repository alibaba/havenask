#ifndef __INDEXLIB_SEGMENTMERGEINFOTEST_H
#define __INDEXLIB_SEGMENTMERGEINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentMergeInfoTest : public INDEXLIB_TESTBASE
{
public:
    SegmentMergeInfoTest();
    ~SegmentMergeInfoTest();

    DECLARE_CLASS_NAME(SegmentMergeInfoTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
    void TestComparation();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentMergeInfoTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(SegmentMergeInfoTest, TestComparation);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENTMERGEINFOTEST_H
