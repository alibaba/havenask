#ifndef __INDEXLIB_DUMPCONTAINERFLUSHEXECUTORTEST_H
#define __INDEXLIB_DUMPCONTAINERFLUSHEXECUTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/open_executor/dump_container_flush_executor.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/partition/online_partition.h"

IE_NAMESPACE_BEGIN(partition);

class DumpContainerFlushExecutorTest : public INDEXLIB_TESTBASE
{
public:
    DumpContainerFlushExecutorTest();
    ~DumpContainerFlushExecutorTest();

    DECLARE_CLASS_NAME(DumpContainerFlushExecutorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestExecuteFailed();
private:
    config::IndexPartitionSchemaPtr mSchema;
    OnlinePartitionPtr mPartition;
    util::PartitionMemoryQuotaControllerPtr mMemController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DumpContainerFlushExecutorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DumpContainerFlushExecutorTest, TestExecuteFailed);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DUMPCONTAINERFLUSHEXECUTORTEST_H
