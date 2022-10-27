#ifndef __INDEXLIB_PARTITIONMEMCONTROLPERFTEST_H
#define __INDEXLIB_PARTITIONMEMCONTROLPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionMemControlPerfTest : public INDEXLIB_TESTBASE
{
public:
    PartitionMemControlPerfTest();
    ~PartitionMemControlPerfTest();

    DECLARE_CLASS_NAME(PartitionMemControlPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    OnlinePartitionPtr CreateOnlinePartition(const std::string& partitionName);
    void CheckMemControlThread();
    void PartitionOpenReopenThread(const std::string partitionName);

private:
    util::MemoryQuotaControllerPtr mMemController;
    config::IndexPartitionSchemaPtr mSchema;
    volatile bool mStop;
private:
    IE_LOG_DECLARE();
};

//INDEXLIB_UNIT_TEST_CASE(PartitionMemControlPerfTest, TestSimpleProcess); // temporary case

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONMEMCONTROLPERFTEST_H
