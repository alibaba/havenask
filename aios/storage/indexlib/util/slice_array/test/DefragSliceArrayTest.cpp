#include "indexlib/util/slice_array/DefragSliceArray.h"

#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
namespace indexlib { namespace util {

class DefragSliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    typedef DefragSliceArray::SliceArray SliceArray;
    typedef DefragSliceArray::SliceArrayPtr SliceArrayPtr;

public:
    DefragSliceArrayTest();
    ~DefragSliceArrayTest();

    DECLARE_CLASS_NAME(DefragSliceArrayTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAppendAndGet();
    void TestAppendAndReuse();
    void TestFree();
    void TestAppendWhenTurnToNextSlice();
    void TestFreeLastSliceSpace();

private:
    SimpleMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendAndGet);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendAndReuse);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestFree);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendWhenTurnToNextSlice);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestFreeLastSliceSpace);

AUTIL_LOG_SETUP(indexlib.util, DefragSliceArrayTest);

DefragSliceArrayTest::DefragSliceArrayTest() {}

DefragSliceArrayTest::~DefragSliceArrayTest() {}

void DefragSliceArrayTest::CaseSetUp()
{
    _memController.reset(new SimpleMemoryQuotaController(MemoryQuotaControllerCreator::CreateBlockMemoryController()));
}

void DefragSliceArrayTest::CaseTearDown() {}

void DefragSliceArrayTest::TestAppendAndGet()
{
    SliceArrayPtr sliceArray(new SliceArray(_memController, 8, 3));
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
    SliceArrayPtr sliceArray(new SliceArray(_memController, 8, 10));
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
    SliceArrayPtr sliceArray(new SliceArray(_memController, 8, 3));
    DefragSliceArray defragArray(sliceArray);

    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    // turn to next slice
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    ASSERT_EQ(2, defragArray.GetWastedSize(0));
}

void DefragSliceArrayTest::TestFree()
{
    SliceArrayPtr sliceArray(new SliceArray(_memController, 16, 4));
    DefragSliceArray defragArray(sliceArray);

    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    defragArray.Free(4, 1);
    ASSERT_EQ(1, defragArray.GetWastedSize(0));

    defragArray.Free(5, 1);
    ASSERT_EQ(2, defragArray.GetWastedSize(0));
}

void DefragSliceArrayTest::TestFreeLastSliceSpace()
{
    SliceArrayPtr sliceArray(new SliceArray(_memController, 8, 3));
    DefragSliceArray defragArray(sliceArray);
    defragArray.FreeLastSliceSpace(-1);
    ASSERT_EQ((uint64_t)4, defragArray.Append("12", 2));
    ASSERT_EQ((uint64_t)12, defragArray.Append("1234", 4));
    defragArray.FreeLastSliceSpace(0);
    ASSERT_EQ(4, defragArray.GetWastedSize(0));
    ASSERT_EQ((uint64_t)20, defragArray.Append("", 0));
    ASSERT_EQ(0, defragArray.GetWastedSize(1));
}
}} // namespace indexlib::util
