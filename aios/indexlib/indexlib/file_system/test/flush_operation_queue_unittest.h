#ifndef __INDEXLIB_FLUSHOPERATIONQUEUETEST_H
#define __INDEXLIB_FLUSHOPERATIONQUEUETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/flush_operation_queue.h"

IE_NAMESPACE_BEGIN(file_system);

class FlushOperationQueueTest : public INDEXLIB_TESTBASE
{
public:
    FlushOperationQueueTest();
    ~FlushOperationQueueTest();

    DECLARE_CLASS_NAME(FlushOperationQueueTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFlushOperation();
    void TestUpdateStorageMetrics();
    void TestCleanCacheAfterDump();
    void TestFlushFileWithFlushCache();
private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestFlushOperation);
INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestUpdateStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestCleanCacheAfterDump);
INDEXLIB_UNIT_TEST_CASE(FlushOperationQueueTest, TestFlushFileWithFlushCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FLUSHOPERATIONQUEUETEST_H
