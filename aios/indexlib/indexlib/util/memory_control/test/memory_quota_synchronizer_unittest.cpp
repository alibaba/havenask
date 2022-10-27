#include "indexlib/util/memory_control/test/memory_quota_synchronizer_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MemoryQuotaSynchronizerTest);

MemoryQuotaSynchronizerTest::MemoryQuotaSynchronizerTest()
{
}

MemoryQuotaSynchronizerTest::~MemoryQuotaSynchronizerTest()
{
}

void MemoryQuotaSynchronizerTest::CaseSetUp()
{
    mMemQuotaController.reset(new MemoryQuotaController(10 * 1024 * 1024)); // 1MB
}

void MemoryQuotaSynchronizerTest::CaseTearDown()
{
}

void MemoryQuotaSynchronizerTest::TestSimpleProcess()
{
    int64_t freeQuota = 10 * 1024 * 1024;
    ASSERT_EQ(freeQuota, mMemQuotaController->GetFreeQuota());
    MemoryQuotaSynchronizerPtr sync1 = CreateOneSynchronizer();
    MemoryQuotaSynchronizerPtr sync2 = CreateOneSynchronizer();
    sync1->SyncMemoryQuota(1024);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE,
              mMemQuotaController->GetFreeQuota());
    sync1->SyncMemoryQuota(512);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE,
              mMemQuotaController->GetFreeQuota());

    sync1->SyncMemoryQuota(4 * 1024 * 1024 + 500);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE * 2,
              mMemQuotaController->GetFreeQuota());
    sync1->SyncMemoryQuota(499);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE,
              mMemQuotaController->GetFreeQuota());

    sync2->SyncMemoryQuota(1024);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE * 2,
              mMemQuotaController->GetFreeQuota());

    sync1.reset();
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE,
              mMemQuotaController->GetFreeQuota());
    sync2.reset();
    ASSERT_EQ(freeQuota, mMemQuotaController->GetFreeQuota());
}

MemoryQuotaSynchronizerPtr MemoryQuotaSynchronizerTest::CreateOneSynchronizer()
{
    PartitionMemoryQuotaControllerPtr partitionMemoryQuotaControl(
            new PartitionMemoryQuotaController(mMemQuotaController,
                    "test_partition_control"));
    
    BlockMemoryQuotaControllerPtr blockMemQuotaControl(
            new BlockMemoryQuotaController(partitionMemoryQuotaControl,
                    "block_test_partition_controller"));
    
    MemoryQuotaSynchronizerPtr memQuotaSync(new MemoryQuotaSynchronizer(blockMemQuotaControl));
    return memQuotaSync;
}

IE_NAMESPACE_END(util);

