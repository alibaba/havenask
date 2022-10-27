#ifndef __INDEXLIB_MMAPLOADSTRATEGYTEST_H
#define __INDEXLIB_MMAPLOADSTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"

IE_NAMESPACE_BEGIN(file_system);

class MmapLoadStrategyTest : public INDEXLIB_TESTBASE {
public:
    MmapLoadStrategyTest();
    ~MmapLoadStrategyTest();

    DECLARE_CLASS_NAME(MmapLoadStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestParse();
    void TestDefaultValue();
    void TestCheck();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MmapLoadStrategyTest, TestParse);
INDEXLIB_UNIT_TEST_CASE(MmapLoadStrategyTest, TestDefaultValue);
INDEXLIB_UNIT_TEST_CASE(MmapLoadStrategyTest, TestCheck);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MMAPLOADSTRATEGYTEST_H
