#include "indexlib/partition/operation_queue/test/remove_operation_unittest.h"

#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/test/partition_data_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RemoveOperationTest);

RemoveOperationTest::RemoveOperationTest() {}

RemoveOperationTest::~RemoveOperationTest() {}

void RemoveOperationTest::CaseSetUp()
{
    mPool.allocate(8); // first allocate will create header(use more memory)

    uint64_t hashValue(12345);
    mOperation.reset(new RemoveOperation<uint64_t>(10));
    mOperation->Init(hashValue, segmentid_t(20));
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void RemoveOperationTest::CaseTearDown() {}
void RemoveOperationTest::TestProcess()
{
    DoTestProcess<uint64_t>();
    DoTestProcess<uint128_t>();
}

void RemoveOperationTest::TestClone()
{
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    RemoveOperation<uint64_t>* clonedRemoveOperation = dynamic_cast<RemoveOperation<uint64_t>*>(clonedOperation);
    ASSERT_TRUE(clonedRemoveOperation);

    uint64_t expectHashValue(12345);
    ASSERT_EQ(expectHashValue, clonedRemoveOperation->mPkHash);
    ASSERT_EQ((int64_t)10, clonedRemoveOperation->mTimestamp);
    ASSERT_EQ((segmentid_t)20, clonedRemoveOperation->mSegmentId);
}

void RemoveOperationTest::TestGetMemoryUse()
{
    size_t memUseBegin = mPool.getUsedBytes();
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    size_t memUseEnd = mPool.getUsedBytes();
    ASSERT_EQ(memUseEnd - memUseBegin, clonedOperation->GetMemoryUse());
}

void RemoveOperationTest::TestSerialize()
{
    char buffer[1024];
    mOperation->Serialize(buffer, 1024);

    Pool pool;
    RemoveOperation<uint64_t> operation(10);
    char* cursor = buffer;
    operation.Load(&pool, cursor);

    ASSERT_EQ(mOperation->mPkHash, operation.mPkHash);
    ASSERT_EQ(mOperation->mTimestamp, operation.mTimestamp);
    ASSERT_EQ(mOperation->mSegmentId, operation.mSegmentId);
}
}} // namespace indexlib::partition
