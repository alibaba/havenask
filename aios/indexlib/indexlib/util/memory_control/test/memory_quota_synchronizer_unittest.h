#ifndef __INDEXLIB_MEMORYQUOTASYNCHRONIZERTEST_H
#define __INDEXLIB_MEMORYQUOTASYNCHRONIZERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/memory_quota_synchronizer.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class MemoryQuotaSynchronizerTest : public INDEXLIB_TESTBASE
{
public:
    MemoryQuotaSynchronizerTest();
    ~MemoryQuotaSynchronizerTest();

    DECLARE_CLASS_NAME(MemoryQuotaSynchronizerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    MemoryQuotaSynchronizerPtr CreateOneSynchronizer();
    
private:
    MemoryQuotaControllerPtr mMemQuotaController;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MemoryQuotaSynchronizerTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMORYQUOTASYNCHRONIZERTEST_H
