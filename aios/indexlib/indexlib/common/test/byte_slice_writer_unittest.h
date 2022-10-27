#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/mem_pool/SimpleAllocator.h>
#include <sstream>

IE_NAMESPACE_BEGIN(common);

class ByteSliceWriterTest : public INDEXLIB_TESTBASE
{
public:
    ByteSliceWriterTest() {}
    ~ByteSliceWriterTest() {}

    DECLARE_CLASS_NAME(ByteSliceWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForWriteInt();
    void TestCaseForWriteManyByte();
    void TestCaseForWriteManyInt16();
    void TestCaseForWriteManyInt32();
    void TestCaseForWrite();
    void TestCaseForWriteByteSliceList();
    void TestCaseForWriteByteSliceListFail();
    void TestCaseForWriteAndWriteByte();
    void TestCaseForHoldInt32();
    void TestCaseForReset();
    void TestCaseForDump();

    //slice size should <= chunk size
    void TestForCreateSlice();


private:
    template<typename T>
    void CheckList(const util::ByteSliceList* list, uint32_t nNumElem, T value = 0);

    void CheckListInBinaryFormat(const util::ByteSliceList* list, const std::string& answer,
                                 uint32_t start, uint32_t end);

    void TestForDump(uint32_t totalBytes);

    void CleanFiles();
        
private:
    std::string mDir;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteInt);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteManyByte);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteManyInt16);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteManyInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteAndWriteByte);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForHoldInt32);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForReset);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteByteSliceList);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForWriteByteSliceListFail);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestCaseForDump);
INDEXLIB_UNIT_TEST_CASE(ByteSliceWriterTest, TestForCreateSlice);


//////////////////////////////////////////////////////////////////////

template<typename T>
inline void ByteSliceWriterTest::CheckList(
        const util::ByteSliceList* list, uint32_t nNumElem, T value)
{
    INDEXLIB_TEST_EQUAL(nNumElem * sizeof(T), list->GetTotalSize());
    uint8_t* buffer = new uint8_t[list->GetTotalSize()];
    size_t n = 0;
    util::ByteSlice* slice = list->GetHead();
    while (slice)
    {
        memcpy(buffer + n, slice->data, slice->size);
        n += slice->size;
        INDEXLIB_TEST_TRUE(n <= list->GetTotalSize());
        slice = slice->next;
    }

    n = 0;        
    uint32_t numInts;
    for (numInts = 0; numInts < nNumElem; numInts++)
    {
        if (value == (T)0)
        {
            INDEXLIB_TEST_EQUAL((T)numInts, *(T*)(buffer + n));
        }
        else 
        {
            INDEXLIB_TEST_EQUAL((T)value, *(T*)(buffer + n));
        }
        n += sizeof(T);
    }
    INDEXLIB_TEST_EQUAL(numInts, nNumElem);
    delete[] buffer;
}

IE_LOG_SETUP(common, ByteSliceWriterTest);

IE_NAMESPACE_END(common);
