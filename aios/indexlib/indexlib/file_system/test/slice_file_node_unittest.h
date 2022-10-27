#ifndef __INDEXLIB_SLICEFILENODETEST_H
#define __INDEXLIB_SLICEFILENODETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/slice_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class SliceFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    SliceFileNodeTest();
    ~SliceFileNodeTest();

    DECLARE_CLASS_NAME(SliceFileNodeTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSliceFileReaderNotSupport();
    void TestDoNotUseRSSBeforeWrite();

private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

// INDEXLIB_UNIT_TEST_CASE(SliceFileNodeTest, TestDoNotUseRSSBeforeWrite);
INDEXLIB_UNIT_TEST_CASE(SliceFileNodeTest, TestSliceFileReaderNotSupport);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICEFILENODETEST_H
