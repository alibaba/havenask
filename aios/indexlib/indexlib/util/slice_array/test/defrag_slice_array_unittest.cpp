#include "indexlib/util/slice_array/test/defrag_slice_array_unittest.h"
#include "indexlib/util/slice_array/bytes_aligned_slice_array.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, DefragSliceArrayTest);

DefragSliceArrayTest::DefragSliceArrayTest()
{
}

DefragSliceArrayTest::~DefragSliceArrayTest()
{
}

void DefragSliceArrayTest::CaseSetUp()
{
    mMemController.reset(new SimpleMemoryQuotaController(
                    MemoryQuotaControllerCreator::CreateBlockMemoryController()));
}

void DefragSliceArrayTest::CaseTearDown()
{
}

void DefragSliceArrayTest::TestAppendAndGet()
{
    SliceArrayPtr sliceArray(new SliceArray(mMemController, 8, 3));
    DefragSliceArray defragArray(sliceArray);
    ASSERT_EQ((size_t)4, sizeof(DefragSliceArray::SliceHeader));

    ASSERT_THROW(defragArray.Append("12345", 5), OutOfRangeException);
    ASSERT_EQ((uint64_t)4, defragArray.Append("1234", 4));
    ASSERT_EQ(string("1234"), string((char*)defragArray.Get(4), 4));
    ASSERT_EQ((uint64_t)12, defragArray.Append("", 0));
    ASSERT_EQ((uint64_t)12, defragArray.Append("1", 1));
    ASSERT_EQ('1', *(char*)defragArray.Get(12));
    ASSERT_EQ((uint64_t)13, defragArray.Append("1", 1));
    ASSERT_EQ('1', *(char*)defragArray.Get(13));
    ASSERT_EQ((uint64_t)20, defragArray.Append("1234", 4));
    ASSERT_ANY_THROW(defragArray.Append("", 0));
    ASSERT_EQ(string("1234"), string((char*)defragArray.Get(20), 4));
}

void DefragSliceArrayTest::TestAppendAndReuse()
{
    SliceArrayPtr sliceArray(new SliceArray(mMemController, 8, 10));
    DefragSliceArray defragArray(sliceArray);
    ASSERT_EQ((size_t)4, sizeof(DefragSliceArray::SliceHeader));

    ASSERT_EQ((uint64_t)4, defragArray.Append("1234", 4));
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    ASSERT_EQ((uint64_t)20, defragArray.Append("1234", 4));
    ASSERT_EQ((uint64_t)28, defragArray.Append("1234", 4));
    defragArray.ReleaseSlice(1);
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    defragArray.ReleaseSlice(0);
    ASSERT_EQ((uint64_t)4, defragArray.Append("", 0));
}

void DefragSliceArrayTest::TestAppendWhenTurnToNextSlice()
{
    SliceArrayPtr sliceArray(new SliceArray(mMemController, 8, 3));
    DefragSliceArray defragArray(sliceArray);

    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    // turn to next slice
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    ASSERT_EQ(2, defragArray.GetWastedSize(0));
}

void DefragSliceArrayTest::TestFree()
{
    SliceArrayPtr sliceArray(new SliceArray(mMemController, 16, 4));
    DefragSliceArray defragArray(sliceArray);
    
    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    defragArray.Free(4, 1);
    ASSERT_EQ(1, defragArray.GetWastedSize(0));

    defragArray.Free(5, 1);
    ASSERT_EQ(2, defragArray.GetWastedSize(0));
}

void DefragSliceArrayTest::TestFreeLastSliceSpace()
{
    SliceArrayPtr sliceArray(new SliceArray(mMemController, 8, 3));
    DefragSliceArray defragArray(sliceArray);
    defragArray.FreeLastSliceSpace(-1);
    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    defragArray.FreeLastSliceSpace(0);
    ASSERT_EQ(4, defragArray.GetWastedSize(0));
    ASSERT_EQ((uint64_t)20, defragArray.Append("", 0));
    ASSERT_EQ(0, defragArray.GetWastedSize(1));
}

IE_NAMESPACE_END(util);

