#define APSARA_UNIT_TEST_MAIN

#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {

class ByteSliceTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ByteSliceTest);
    void CaseSetUp() override {}

    void CaseTearDown() override { _byteSliceList.Clear(NULL); }

    void TestCaseForAdd()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        size = 1100 - ByteSlice::GetHeadSize();
        ByteSlice* slice2 = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);
        _byteSliceList.Add(slice2);
        uint32_t totalSize = _byteSliceList.GetTotalSize();
        ASSERT_TRUE(totalSize == 2124 - 2 * ByteSlice::GetHeadSize());

        _byteSliceList.Clear(NULL);
    }

    void TestCaseForGetHead()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        ByteSlice* slice2 = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);
        _byteSliceList.Add(slice2);
        ASSERT_TRUE(_byteSliceList.GetHead() == slice);
        _byteSliceList.Clear(NULL);
    }

    void TestCaseForClear()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);
        _byteSliceList.Clear(NULL);
        ASSERT_TRUE(_byteSliceList.GetTotalSize() == 0);
    }

    void TestCaseForUpdateTotalSize()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);

        slice = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);

        slice = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);

        ASSERT_EQ(size * 3, _byteSliceList.UpdateTotalSize());

        slice = _byteSliceList.GetHead();
        while (slice != NULL) {
            slice->size = slice->size - 10;
            slice = slice->next;
        }
        ASSERT_EQ((size - 10) * 3, _byteSliceList.UpdateTotalSize());

        _byteSliceList.Clear(NULL);
    }

    void TestCaseForMerge()
    {
        uint32_t size = 100 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        _byteSliceList.Add(slice);

        ByteSliceList other;
        slice = ByteSlice::CreateObject(size);
        other.Add(slice);

        ByteSlice* tail = other.GetTail();
        _byteSliceList.MergeWith(other);
        ASSERT_EQ(_byteSliceList.GetTail(), tail);
        ASSERT_EQ(_byteSliceList.GetTotalSize(), size * 2);
        ASSERT_EQ((uint32_t)0, other.GetTotalSize());
        ASSERT_TRUE(other.GetHead() == NULL);
    }

    // void TestCaseForMount()
    // {
    //     const size_t TOTAL_LEN = 10240;
    //     uint8_t data[TOTAL_LEN];
    //     for (size_t i = 0; i < TOTAL_LEN; ++i)
    //     {
    //         data[i] = i % 11;
    //     }

    //     ByteSliceListPtr sliceListPtr;
    //     uint8_t* answer = data;

    //     sliceListPtr = CreateByteSliceList(data, TOTAL_LEN, TOTAL_LEN);
    //     CheckMount(sliceListPtr, answer, TOTAL_LEN);

    //     sliceListPtr = CreateByteSliceList(data, TOTAL_LEN, TOTAL_LEN / 4);
    //     CheckMount(sliceListPtr, answer, TOTAL_LEN);

    //     sliceListPtr = CreateByteSliceList(data, TOTAL_LEN, TOTAL_LEN / 4 - 1);
    //     CheckMount(sliceListPtr, answer, TOTAL_LEN);

    //     size_t offset = TOTAL_LEN / 2;
    //     size_t len = TOTAL_LEN / 2 + 1;
    //     sliceListPtr = CreateByteSliceList(data, TOTAL_LEN, TOTAL_LEN / 5 - 1);

    //     ByteSliceList list;
    //     INDEXLIB_EXPECT_EXCEPTION(list.Mount(sliceListPtr.get(), offset, len),
    //             OutOfRangeException);
    //     list.Clear(NULL);
    // }

private:
    ByteSliceListPtr CreateByteSliceList(uint8_t* data, size_t totalLen, size_t blockLen)
    {
        ByteSliceListPtr byteSliceList(new ByteSliceList);
        for (size_t offset = 0; offset < totalLen; offset += blockLen) {
            size_t curLen = blockLen;
            if (offset + curLen > totalLen) {
                curLen = totalLen - offset;
            }

            ByteSlice* slice = ByteSlice::CreateObject(curLen);
            memcpy(slice->data, data + offset, curLen);
            byteSliceList->Add(slice);
        }

        return byteSliceList;
    }

    // void CheckMount(const ByteSliceListPtr& byteSliceList,
    //                            uint8_t* answer,
    //                            size_t totalLen)
    // {
    //     vector<uint32_t> steps;
    //     steps.push_back(1);
    //     steps.push_back(totalLen / 2);
    //     steps.push_back(totalLen);

    //     for (size_t i = 0; i < steps.size(); ++i)
    //     {
    //         for (size_t offset = 0; offset < totalLen; offset += steps[i])
    //         {
    //             size_t curLen = steps[i];
    //             if (offset + curLen > totalLen)
    //             {
    //                 curLen = totalLen - offset;
    //             }
    //             ByteSliceList list;
    //             list.Mount(byteSliceList.get(), offset, curLen);
    //             AssertEqual(list, answer + offset, curLen);
    //             list.Clear(NULL);
    //         }
    //     }
    // }

    void AssertEqual(const ByteSliceList& list, uint8_t* answer, size_t len)
    {
        ByteSlice* cursorNode = list.GetHead();
        size_t offset = 0;
        while (cursorNode != NULL) {
            int32_t cmpResult = memcmp(cursorNode->data, (void*)(answer + offset), cursorNode->size);
            ASSERT_EQ(0, cmpResult);

            offset += cursorNode->size;
            cursorNode = cursorNode->next;
        }

        ASSERT_EQ(len, offset);
    }

private:
    ByteSliceList _byteSliceList;
};

INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForAdd);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForClear);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForGetHead);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForUpdateTotalSize);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForMerge);
// INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForMount);
}} // namespace indexlib::util
