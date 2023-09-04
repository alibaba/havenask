#include "indexlib/partition/operation_queue/test/update_field_operation_unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, UpdateFieldOperationTest);

UpdateFieldOperationTest::UpdateFieldOperationTest() : mPool(10 * 1024 * 1024, 1) {}

UpdateFieldOperationTest::~UpdateFieldOperationTest() {}

void UpdateFieldOperationTest::CaseSetUp()
{
    mOperation = MakeOperation<uint64_t>();
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void UpdateFieldOperationTest::CaseTearDown() {}

void UpdateFieldOperationTest::TestClone()
{
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    UpdateFieldOperation<uint64_t>* clonedUpdateOperation =
        dynamic_cast<UpdateFieldOperation<uint64_t>*>(clonedOperation);
    ASSERT_TRUE(clonedUpdateOperation);

    uint64_t expectHashValue(12345);
    ASSERT_EQ(expectHashValue, clonedUpdateOperation->mPkHash);
    ASSERT_EQ((int64_t)10, clonedUpdateOperation->mTimestamp);
    ASSERT_EQ((size_t)1, clonedUpdateOperation->mItemSize);
    ASSERT_TRUE(mOperation->mItems != clonedUpdateOperation->mItems);
    ASSERT_EQ(*mOperation->mItems, *clonedUpdateOperation->mItems);
}

void UpdateFieldOperationTest::TestSerialize()
{
    char buffer[1024];
    mOperation->Serialize(buffer, 1024);

    Pool pool;
    UpdateFieldOperation<uint64_t> operation(10);
    char* cursor = buffer;
    operation.Load(&pool, cursor);

    uint64_t expectHashValue(12345);
    ASSERT_EQ(expectHashValue, operation.mPkHash);
    ASSERT_EQ((int64_t)10, operation.mTimestamp);
    ASSERT_EQ((size_t)1, operation.mItemSize);
    ASSERT_EQ(*mOperation->mItems, *operation.mItems);
}

// FIXME: this test case triggers gcc internal compile error
void UpdateFieldOperationTest::TestProcess()
{
#if defined(__clang__)
    DoTestProcess<uint64_t>(false);
    DoTestProcess<uint128_t>(false);

    DoTestProcess<uint64_t>(true);
    DoTestProcess<uint128_t>(true);
#endif
}

void UpdateFieldOperationTest::TestGetMemoryUse()
{
    size_t memUseBegin = mPool.getUsedBytes();
    OperationBase* clonedOperation = mOperation->Clone(&mPool);
    size_t memUseEnd = mPool.getUsedBytes();

    // each StringView add copy more '\0'
    ASSERT_EQ(memUseEnd - memUseBegin, clonedOperation->GetMemoryUse() + 1);
}
}} // namespace indexlib::partition
