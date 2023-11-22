#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"

namespace indexlib { namespace partition {

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

// INDEXLIB_UNIT_TEST_CASE(PartitionMemControlPerfTest, TestSimpleProcess); // temporary case
}} // namespace indexlib::partition
