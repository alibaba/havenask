#ifndef __INDEXLIB_CUSTOMIZEDINDEXMERGERTEST_H
#define __INDEXLIB_CUSTOMIZEDINDEXMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_merger.h"

IE_NAMESPACE_BEGIN(index);

class CustomizedIndexMergerTest : public INDEXLIB_TESTBASE
{
public:
    CustomizedIndexMergerTest();
    ~CustomizedIndexMergerTest();

    DECLARE_CLASS_NAME(CustomizedIndexMergerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMultiOutputSegment();
    void TestParallelReduceMeta();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexMergerTest, TestMultiOutputSegment);
INDEXLIB_UNIT_TEST_CASE(CustomizedIndexMergerTest, TestParallelReduceMeta);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZEDINDEXMERGERTEST_H
