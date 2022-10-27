#define APSARA_UNIT_TEST_MAIN

#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);

class ByteSliceTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ByteSliceTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
        mByteSliceList.Clear(NULL);
    }

    void TestCaseForAdd()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        size = 1100 - ByteSlice::GetHeadSize();
        ByteSlice* slice2 = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);
        mByteSliceList.Add(slice2);
        uint32_t totalSize = mByteSliceList.GetTotalSize();
        INDEXLIB_TEST_TRUE(totalSize == 2124 - 2 * ByteSlice::GetHeadSize());
        
        mByteSliceList.Clear(NULL);
    }

    void TestCaseForGetHead()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        ByteSlice* slice2 = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);
        mByteSliceList.Add(slice2);
        INDEXLIB_TEST_TRUE(mByteSliceList.GetHead() == slice);
        mByteSliceList.Clear(NULL);
    }

    void TestCaseForClear()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);
        mByteSliceList.Clear(NULL);
        INDEXLIB_TEST_TRUE(mByteSliceList.GetTotalSize() == 0);
    }

    void TestCaseForUpdateTotalSize()
    {
        uint32_t size = 1024 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);

        slice = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);

        slice = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);

        INDEXLIB_TEST_EQUAL(size * 3, mByteSliceList.UpdateTotalSize());

        slice = mByteSliceList.GetHead();
        while (slice != NULL)
        {
            slice->size -= 10;
            slice = slice->next;
        }
        INDEXLIB_TEST_EQUAL((size - 10)* 3, mByteSliceList.UpdateTotalSize());
        
        mByteSliceList.Clear(NULL);
    }
    
    void TestCaseForMerge()
    {
        uint32_t size = 100 - ByteSlice::GetHeadSize();
        ByteSlice* slice = ByteSlice::CreateObject(size);
        mByteSliceList.Add(slice);
        
        ByteSliceList other;
        slice = ByteSlice::CreateObject(size);
        other.Add(slice);

        ByteSlice* tail = other.GetTail();
        mByteSliceList.MergeWith(other);
        INDEXLIB_TEST_EQUAL(mByteSliceList.GetTail(), tail);
        INDEXLIB_TEST_EQUAL(mByteSliceList.GetTotalSize(), size * 2);
        INDEXLIB_TEST_EQUAL((uint32_t)0, other.GetTotalSize());
        INDEXLIB_TEST_TRUE(other.GetHead() == NULL);
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
        for (size_t offset = 0; offset < totalLen; offset += blockLen)
        {
            size_t curLen = blockLen;
            if (offset + curLen > totalLen)
            {
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
        while (cursorNode != NULL)
        {
            int32_t cmpResult =  memcmp(cursorNode->data,
                    (void*)(answer + offset), cursorNode->size);
            INDEXLIB_TEST_EQUAL(0, cmpResult);

            offset += cursorNode->size;
            cursorNode = cursorNode->next;
        }

        INDEXLIB_TEST_EQUAL(len, offset);
    }

private:
    ByteSliceList mByteSliceList;
};

INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForAdd);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForClear);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForGetHead);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForUpdateTotalSize);
INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForMerge);
// INDEXLIB_UNIT_TEST_CASE(ByteSliceTest, TestCaseForMount);

IE_NAMESPACE_END(util);
