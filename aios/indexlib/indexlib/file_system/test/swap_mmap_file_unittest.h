#ifndef __INDEXLIB_SWAPMMAPFILETEST_H
#define __INDEXLIB_SWAPMMAPFILETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/swap_mmap_file_reader.h"
#include "indexlib/file_system/swap_mmap_file_writer.h"

IE_NAMESPACE_BEGIN(file_system);

class SwapMmapFileTest : public INDEXLIB_TESTBASE
{
public:
    SwapMmapFileTest();
    ~SwapMmapFileTest();

    DECLARE_CLASS_NAME(SwapMmapFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestSimpleProcess(bool autoStore);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SwapMmapFileTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SWAPMMAPFILETEST_H
