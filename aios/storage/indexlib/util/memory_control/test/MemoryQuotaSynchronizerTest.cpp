#include "indexlib/util/memory_control/MemoryQuotaSynchronizer.h"

#include "autil/Log.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
namespace indexlib { namespace util {

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
    MemoryQuotaControllerPtr _memQuotaController;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MemoryQuotaSynchronizerTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.util, MemoryQuotaSynchronizerTest);

MemoryQuotaSynchronizerTest::MemoryQuotaSynchronizerTest() {}

MemoryQuotaSynchronizerTest::~MemoryQuotaSynchronizerTest() {}

void MemoryQuotaSynchronizerTest::CaseSetUp()
{
    _memQuotaController.reset(new MemoryQuotaController(10 * 1024 * 1024)); // 1MB
}

void MemoryQuotaSynchronizerTest::CaseTearDown() {}

void MemoryQuotaSynchronizerTest::TestSimpleProcess()
{
    int64_t freeQuota = 10 * 1024 * 1024;
    ASSERT_EQ(freeQuota, _memQuotaController->GetFreeQuota());
    MemoryQuotaSynchronizerPtr sync1 = CreateOneSynchronizer();
    MemoryQuotaSynchronizerPtr sync2 = CreateOneSynchronizer();
    sync1->SyncMemoryQuota(1024);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE, _memQuotaController->GetFreeQuota());
    sync1->SyncMemoryQuota(512);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE, _memQuotaController->GetFreeQuota());

    sync1->SyncMemoryQuota(4 * 1024 * 1024 + 500);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE * 2, _memQuotaController->GetFreeQuota());
    sync1->SyncMemoryQuota(499);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE, _memQuotaController->GetFreeQuota());

    sync2->SyncMemoryQuota(1024);
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE * 2, _memQuotaController->GetFreeQuota());

    sync1.reset();
    ASSERT_EQ(freeQuota - BlockMemoryQuotaController::BLOCK_SIZE, _memQuotaController->GetFreeQuota());
    sync2.reset();
    ASSERT_EQ(freeQuota, _memQuotaController->GetFreeQuota());
}

MemoryQuotaSynchronizerPtr MemoryQuotaSynchronizerTest::CreateOneSynchronizer()
{
    PartitionMemoryQuotaControllerPtr partitionMemoryQuotaControl(
        new PartitionMemoryQuotaController(_memQuotaController, "test_partition_control"));

    BlockMemoryQuotaControllerPtr blockMemQuotaControl(
        new BlockMemoryQuotaController(partitionMemoryQuotaControl, "block_test_partition_controller"));

    MemoryQuotaSynchronizerPtr memQuotaSync(new MemoryQuotaSynchronizer(blockMemQuotaControl));
    return memQuotaSync;
}
}} // namespace indexlib::util
