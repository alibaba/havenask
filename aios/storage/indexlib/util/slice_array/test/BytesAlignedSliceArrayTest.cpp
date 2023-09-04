#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

#include "indexlib/util/Exception.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class BytesAlignedSliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    BytesAlignedSliceArrayTest();
    ~BytesAlignedSliceArrayTest();

    DECLARE_CLASS_NAME(BytesAlignedSliceArrayTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForZero();
    void TestCaseForSliceLength();
    void TestCaseForSimple();
    void TestCaseForMultiSlice();
    void TestCaseForRandomSmallSlice();
    void TestCaseForRandomBigSlice();
    // void TestCaseForStorageMetrics();
    void TestCaseForSingleSlice();
    void TestCaseForExtendException();
    void TestCaseForMassData();
    void TestAllocateSlice();
    void TestReleaseSlice();

private:
    template <typename T>
    void testRandom(size_t sliceLen, size_t valueCount);

private:
    SimpleMemoryQuotaControllerPtr _memoryController;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForZero);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSliceLength);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForMultiSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForRandomSmallSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForRandomBigSlice);
// INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSingleSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForExtendException);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForMassData);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestAllocateSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestReleaseSlice);

AUTIL_LOG_SETUP(indexlib.util, BytesAlignedSliceArrayTest);

BytesAlignedSliceArrayTest::BytesAlignedSliceArrayTest() {}

BytesAlignedSliceArrayTest::~BytesAlignedSliceArrayTest() {}

void BytesAlignedSliceArrayTest::CaseSetUp()
{
    _memoryController.reset(
        new SimpleMemoryQuotaController(MemoryQuotaControllerCreator::CreateBlockMemoryController()));
}

void BytesAlignedSliceArrayTest::CaseTearDown() {}

void BytesAlignedSliceArrayTest::TestCaseForZero()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 8, 2);
    uint64_t offset = sliceArray.Insert("", 0);
    ASSERT_EQ((uint64_t)0, offset);
    ASSERT_TRUE(sliceArray.GetValueAddress(0));
    offset = sliceArray.Insert("", 0);
    ASSERT_EQ((uint64_t)0, offset);
    ASSERT_TRUE(sliceArray.GetValueAddress(0));

    offset = sliceArray.Insert("12345678", 8);
    ASSERT_EQ((uint64_t)0, offset);
    const uint8_t* addr = (const uint8_t*)sliceArray.GetValueAddress(0);
    ASSERT_TRUE(addr);

    offset = sliceArray.Insert("", 0);
    ASSERT_EQ((uint64_t)8, offset);
    const uint8_t* addr2 = (const uint8_t*)sliceArray.GetValueAddress(8);
    ASSERT_TRUE(addr2);
    ASSERT_NE(addr, addr2);
}

void BytesAlignedSliceArrayTest::TestCaseForSliceLength()
{
    BytesAlignedSliceArray sliceArray(_memoryController, ((uint64_t)1 << 31) - 1, 2);
    ASSERT_EQ(((size_t)1 << 31), sliceArray.GetSliceLen());
    ASSERT_EQ((size_t)0, sliceArray._usedBytes);
    ASSERT_EQ(((uint64_t)1 << 31) + 1, sliceArray._curCursor);
    ASSERT_EQ(-1, sliceArray._curSliceIdx);
}

void BytesAlignedSliceArrayTest::TestCaseForSimple()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 64 * 1024, 2);

    ASSERT_EQ((size_t)0, sliceArray._usedBytes);
    ASSERT_EQ((size_t)64 * 1024, sliceArray.GetSliceLen());

    uint64_t offset = sliceArray.Insert("ABC", 3);
    ASSERT_EQ((size_t)3, sliceArray._usedBytes);
    ASSERT_EQ((size_t)64 * 1024, sliceArray.GetSliceLen());
    ASSERT_EQ((uint64_t)0, offset);

    offset = sliceArray.Insert("DEFG", 4);
    ASSERT_EQ((size_t)7, sliceArray._usedBytes);
    ASSERT_EQ((size_t)64 * 1024, sliceArray.GetSliceLen());
    ASSERT_EQ((uint64_t)3, offset);

    const char* ptr = (const char*)sliceArray.GetValueAddress(0);
    ASSERT_EQ('A', ptr[0]);
    ASSERT_EQ('B', ptr[1]);
    ASSERT_EQ('C', ptr[2]);
    ASSERT_EQ('D', ptr[3]);
}

void BytesAlignedSliceArrayTest::TestCaseForMultiSlice()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 3, 64 * 1024);

    ASSERT_EQ((size_t)0, sliceArray._usedBytes);
    ASSERT_EQ((size_t)4, sliceArray.GetSliceLen());

    uint64_t offset = sliceArray.Insert("ABC", 3);
    ASSERT_EQ((size_t)3, sliceArray._usedBytes);
    ASSERT_EQ((size_t)4, sliceArray.GetSliceLen());
    ASSERT_EQ((uint64_t)0, offset);

    offset = sliceArray.Insert("DEFG", 4);
    ASSERT_EQ((size_t)7, sliceArray._usedBytes);
    ASSERT_EQ((size_t)4, sliceArray.GetSliceLen());
    ASSERT_EQ((uint64_t)4, offset);

    const char* ptr = (const char*)sliceArray.GetValueAddress(0);
    ASSERT_EQ('A', ptr[0]);
    ASSERT_EQ('B', ptr[1]);
    ASSERT_EQ('C', ptr[2]);

    ptr = (const char*)sliceArray.GetValueAddress(4);
    ASSERT_EQ('D', ptr[0]);
    ASSERT_EQ('E', ptr[1]);
    ASSERT_EQ('F', ptr[2]);
    ASSERT_EQ('G', ptr[3]);
}

void BytesAlignedSliceArrayTest::TestCaseForRandomSmallSlice() { testRandom<int32_t>(4 * 1024, 1000); }

void BytesAlignedSliceArrayTest::TestCaseForRandomBigSlice() { testRandom<int64_t>(64 * 1024, 1000); }

void BytesAlignedSliceArrayTest::TestCaseForSingleSlice()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 5, 1);

    ASSERT_EQ((uint64_t)8, sliceArray.GetSliceLen());
    ASSERT_EQ((size_t)0, sliceArray._usedBytes);
    ASSERT_EQ((uint64_t)9, sliceArray._curCursor);
    ASSERT_EQ(-1, sliceArray._curSliceIdx);

    uint64_t offset = sliceArray.Insert("12345", 5);
    ASSERT_EQ((uint64_t)8, sliceArray.GetSliceLen());
    ASSERT_EQ((size_t)5, sliceArray._usedBytes);
    ASSERT_EQ((uint64_t)5, sliceArray._curCursor);
    ASSERT_EQ(0, sliceArray._curSliceIdx);

    const char* ptr = (const char*)sliceArray.GetValueAddress(offset);
    ASSERT_EQ('1', ptr[0]);
    ASSERT_EQ('2', ptr[1]);
    ASSERT_EQ('3', ptr[2]);
    ASSERT_EQ('4', ptr[3]);
    ASSERT_EQ('5', ptr[4]);

    ASSERT_EQ((int64_t)5, sliceArray.Insert("123", 3));
}

void BytesAlignedSliceArrayTest::TestAllocateSlice()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 2, 10);
    ASSERT_THROW(sliceArray.ReleaseSlice(0), OutOfRangeException);
    sliceArray.AllocateSlice();
    ASSERT_EQ((int64_t)0, sliceArray._curSliceIdx);
    sliceArray.ReleaseSlice(0);
    ASSERT_EQ(0, sliceArray.Insert("12", 2));
    ASSERT_EQ((int64_t)0, sliceArray._curSliceIdx);
    ASSERT_EQ(2, sliceArray.Insert("12", 2));
    ASSERT_EQ((int64_t)1, sliceArray._curSliceIdx);
    ASSERT_EQ(4, sliceArray.Insert("12", 2));
    ASSERT_EQ((int64_t)2, sliceArray._curSliceIdx);
    sliceArray.ReleaseSlice(1);
    ASSERT_EQ((int64_t)2, sliceArray._curSliceIdx);
    ASSERT_EQ(2, sliceArray.Insert("", 0));
    ASSERT_EQ((int64_t)1, sliceArray._curSliceIdx);
    ASSERT_EQ(2, sliceArray.Insert("12", 2));
    ASSERT_EQ((int64_t)1, sliceArray._curSliceIdx);
    ASSERT_EQ(6, sliceArray.Insert("12", 2));
    ASSERT_EQ((int64_t)3, sliceArray._curSliceIdx);
}

void BytesAlignedSliceArrayTest::TestReleaseSlice()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 2, 10);
    ASSERT_EQ((int64_t)0, sliceArray.Insert("ab", 2));
    ASSERT_EQ((int64_t)2, sliceArray.Insert(NULL, 0));
    ASSERT_EQ((int64_t)2, sliceArray.Insert("a", 1));
    ASSERT_EQ((int64_t)3, sliceArray.Insert(NULL, 0));
    ASSERT_EQ((uint64_t)3, sliceArray.GetTotalUsedBytes());
    sliceArray.ReleaseSlice(1);
    ASSERT_EQ((uint64_t)2, sliceArray.GetTotalUsedBytes());
    sliceArray.ReleaseSlice(0);
    ASSERT_EQ((uint64_t)0, sliceArray.GetTotalUsedBytes());
}

template <typename T>
void BytesAlignedSliceArrayTest::testRandom(size_t sliceLen, size_t valueCount)
{
    BytesAlignedSliceArray sliceArray(_memoryController, sliceLen, 64 * 1024);

    vector<vector<T>> expectedVec;
    vector<uint64_t> offsetVec;
    for (size_t i = 0; i < valueCount; ++i) {
        vector<T> values;
        uint16_t count = rand() % 1000;
        for (size_t j = 0; j < count; ++j) {
            T value = (T)rand();
            values.push_back(value);
        }
        expectedVec.push_back(values);
        uint64_t offset = sliceArray.Insert((char*)(&values[0]), values.size() * sizeof(T));
        offsetVec.push_back(offset);
    }

    for (size_t i = 0; i < expectedVec.size(); ++i) {
        T* values = (T*)sliceArray.GetValueAddress(offsetVec[i]);
        const vector<T>& expectedValues = expectedVec[i];
        for (size_t j = 0; j < expectedValues.size(); ++j) {
            ASSERT_EQ(expectedValues[j], values[j]);
        }
    }
}

// void BytesAlignedSliceArrayTest::TestCaseForStorageMetrics()
// {
//     StorageMetrics metrics;
//     BytesAlignedSliceArray sliceArray(_memoryController, 8, 2);
//     sliceArray.SetStorageMetrics(&metrics);
//     sliceArray.Insert("a", 0);
//     ASSERT_EQ(0, metrics.GetFileLength(FSFT_SLICE));
//     sliceArray.Insert("1234567", 7);
//     ASSERT_EQ(7, metrics.GetFileLength(FSFT_SLICE));
//     sliceArray.Insert("abcd", 4);
//     ASSERT_EQ(11, metrics.GetFileLength(FSFT_SLICE));
// }

void BytesAlignedSliceArrayTest::TestCaseForExtendException()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 8, 1);
    ASSERT_NO_THROW(sliceArray.Insert("12345678", 8));
    ASSERT_THROW(sliceArray.Insert("1", 1), OutOfRangeException);
}

void BytesAlignedSliceArrayTest::TestCaseForMassData()
{
    BytesAlignedSliceArray sliceArray(_memoryController, 64 * 1024, 8 * 1024);
#define BUFFER_LEN 1024
    char buffer[BUFFER_LEN] = {0};
    size_t totalDataSize = (size_t)8 * 1024 * 1024;
    uint64_t expectOffset = 0;
    for (size_t i = 0; i < totalDataSize / BUFFER_LEN; i++) {
        uint64_t offset = sliceArray.Insert(buffer, BUFFER_LEN);
        ASSERT_EQ(expectOffset, offset);
        expectOffset += BUFFER_LEN;
    }
}
}} // namespace indexlib::util
