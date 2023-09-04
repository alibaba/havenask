#include "indexlib/file_system/ByteSliceWriter.h"

#include "gtest/gtest.h"
#include <memory>
#include <sstream>
#include <stdlib.h>
#include <sys/types.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace common {

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

    // slice size should <= chunk size
    void TestForCreateSlice();

private:
    template <typename T>
    void CheckList(const util::ByteSliceList* list, uint32_t nNumElem, T value = 0);

    void CheckListInBinaryFormat(const util::ByteSliceList* list, const std::string& answer, uint32_t start,
                                 uint32_t end);

    void TestForDump(uint32_t totalBytes);

    void CleanFiles();

private:
    std::string _dir;
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, ByteSliceWriterTest);

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

template <typename T>
inline void ByteSliceWriterTest::CheckList(const util::ByteSliceList* list, uint32_t nNumElem, T value)
{
    ASSERT_EQ(nNumElem * sizeof(T), list->GetTotalSize());
    uint8_t* buffer = new uint8_t[list->GetTotalSize()];
    size_t n = 0;
    util::ByteSlice* slice = list->GetHead();
    while (slice) {
        memcpy(buffer + n, slice->data, slice->size);
        n += slice->size;
        ASSERT_TRUE(n <= list->GetTotalSize());
        slice = slice->next;
    }

    n = 0;
    uint32_t numInts;
    for (numInts = 0; numInts < nNumElem; numInts++) {
        if (value == (T)0) {
            ASSERT_EQ((T)numInts, *(T*)(buffer + n));
        } else {
            ASSERT_EQ((T)value, *(T*)(buffer + n));
        }
        n += sizeof(T);
    }
    ASSERT_EQ(numInts, nNumElem);
    delete[] buffer;
}

void ByteSliceWriterTest::CaseSetUp() { _dir = GET_TEMP_DATA_PATH(); }

void ByteSliceWriterTest::CaseTearDown() {}

void ByteSliceWriterTest::TestCaseForWriteInt()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    uint32_t size = 0;

    // test write byte
    uint8_t byteValue = 12;
    writer.WriteByte(byteValue);
    size += sizeof(byteValue);
    ASSERT_EQ(size, writer.GetSize());

    // test write int16_t
    int16_t int16Value = 120;
    writer.WriteInt16(int16Value);
    size += sizeof(int16_t);
    ASSERT_EQ(size, writer.GetSize());

    // test write int32_t
    int32_t int32Value = 12;
    writer.WriteInt32(int32Value);
    size += sizeof(int32_t);
    ASSERT_EQ(size, writer.GetSize());

    // test write uint32_t
    uint32_t uint32Value = 21;
    writer.WriteUInt32(uint32Value);
    size += sizeof(uint32_t);
    ASSERT_EQ(size, writer.GetSize());
}

void ByteSliceWriterTest::TestCaseForWriteManyByte()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    int32_t numInts = 0;
    for (numInts = 0; numInts < 40000; numInts++) {
        writer.WriteByte((uint8_t)numInts);
    }
    ASSERT_EQ(numInts * sizeof(uint8_t), writer.GetSize());
    CheckList<uint8_t>(writer.GetByteSliceList(), numInts);

    AUTIL_LOG(TRACE1, "End");
}

void ByteSliceWriterTest::TestCaseForWriteManyInt16()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    int16_t numInts = 0;
    for (numInts = 0; numInts < 10000; numInts++) {
        writer.WriteInt16(numInts);
    }
    ASSERT_EQ(numInts * sizeof(int16_t), writer.GetSize());
    CheckList<int16_t>(writer.GetByteSliceList(), numInts);
}

void ByteSliceWriterTest::TestCaseForWriteManyInt32()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    int32_t numInts = 0;
    for (numInts = 0; numInts < 10000; numInts++) {
        writer.WriteInt32(numInts);
    }
    ASSERT_EQ(numInts * sizeof(int32_t), writer.GetSize());
    CheckList<int32_t>(writer.GetByteSliceList(), numInts);
}

void ByteSliceWriterTest::TestCaseForWrite()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    string str(16000, 'x');
    writer.Write(str.c_str(), str.size());
    ASSERT_EQ(str.size(), writer.GetSize());
    CheckList<char>(writer.GetByteSliceList(), 16000, 'x');
}

void ByteSliceWriterTest::TestCaseForWriteByteSliceList()
{
    uint32_t totalBytes = 800;
    stringstream answerStream;
    for (uint32_t i = 0; i < totalBytes; ++i) {
        answerStream << char(i % 256);
    }
    ByteSliceWriter writer;
    writer.Write(answerStream.str().data(), answerStream.str().size());
    ByteSliceList* oriByteSliceList = writer.GetByteSliceList();
    uint32_t start, end;
    for (start = 0; start < totalBytes - 1; ++start) {
        for (end = start + 1; end < totalBytes; ++end) {
            ByteSliceWriter writer2;
            writer2.Write(*oriByteSliceList, start, end);
            CheckListInBinaryFormat(writer2.GetByteSliceList(), answerStream.str(), start, end);
        }
    }
}

void ByteSliceWriterTest::TestCaseForWriteByteSliceListFail()
{
    uint32_t totalBytes = 1600;
    stringstream ss;
    for (uint32_t i = 0; i < totalBytes; ++i) {
        ss << char(i % 256);
    }
    ByteSliceWriter writer;
    writer.Write(ss.str().data(), ss.str().size());
    ByteSliceList* oriByteSliceList = writer.GetByteSliceList();
    ByteSliceWriter writer2;
    bool exceptionCatched = false;
    try {
        writer2.Write(*oriByteSliceList, 1, 1);
    } catch (OutOfRangeException& e) {
        exceptionCatched = true;
    }
    ASSERT_TRUE(exceptionCatched);

    exceptionCatched = false;
    try {
        writer2.Write(*oriByteSliceList, 0, totalBytes + 1);
    } catch (OutOfRangeException& e) {
        exceptionCatched = true;
    }
    ASSERT_TRUE(exceptionCatched);
}

void ByteSliceWriterTest::TestCaseForWriteAndWriteByte()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    writer.WriteByte('x');
    string str(16000, 'x');
    writer.Write(str.c_str(), str.size());

    ASSERT_EQ(str.size() + sizeof(int8_t), writer.GetSize());
    CheckList<char>(writer.GetByteSliceList(), 16001, 'x');
}

void ByteSliceWriterTest::TestCaseForHoldInt32()
{
    ByteSliceWriter writer;
    uint32_t size = 0;

    int32_t& valueHold = writer.HoldInt32();
    size += sizeof(int32_t);
    ASSERT_EQ(size, writer.GetSize());
    writer.WriteInt32(12);
    valueHold = 12;

    CheckList<int32_t>(writer.GetByteSliceList(), 2, 12);
}

void ByteSliceWriterTest::TestCaseForReset()
{
    AUTIL_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    string str(1600, 'x');
    writer.Write(str.c_str(), str.size());
    ASSERT_EQ(str.size(), writer.GetSize());

    writer.Reset();
    ASSERT_EQ((size_t)0, writer.GetSize());
}

void ByteSliceWriterTest::TestCaseForDump()
{
    TestForDump(0);
    // TestForDump(1);
    // TestForDump(ByteSliceWriter::MIN_SLICE_SIZE);
    // TestForDump(ByteSliceWriter::MIN_SLICE_SIZE + 2);
    // TestForDump(3095);
}

// slice size should <= chunk size
void ByteSliceWriterTest::TestForCreateSlice()
{
    uint32_t chunkSize = indexlibv2::DEFAULT_CHUNK_SIZE * 1024 * 1024;
    Pool* byteSlicePool = new Pool(chunkSize);

    {
        ByteSliceWriter writer(byteSlicePool);
        size_t valueSize = 100 * 1024 * 1024;
        uint8_t value = 0;

        for (uint32_t i = 0; i < valueSize; i++) {
            writer.WriteByte(value);
        }
        ASSERT_EQ(valueSize, writer.GetSize());
    }

    delete byteSlicePool;
}
void ByteSliceWriterTest::CheckListInBinaryFormat(const ByteSliceList* list, const string& answer, uint32_t start,
                                                  uint32_t end)
{
    ASSERT_EQ(end - start, list->GetTotalSize());
    uint8_t* buffer = new uint8_t[list->GetTotalSize()];
    size_t n = 0;
    ByteSlice* slice = list->GetHead();
    while (slice) {
        memcpy(buffer + n, slice->data, slice->size);
        n += slice->size;
        ASSERT_TRUE(n <= list->GetTotalSize());
        slice = slice->next;
    }

    int ret = memcmp(buffer, answer.substr(start, end - start).data(), end - start);
    ASSERT_TRUE(ret == 0);
    delete[] buffer;
}

void ByteSliceWriterTest::TestForDump(uint32_t totalBytes)
{
    CleanFiles();

    ByteSliceWriter writer;
    stringstream answerStream;
    for (uint32_t i = 0; i < totalBytes; ++i) {
        writer.WriteByte(char(i % 256));
        answerStream << char(i % 256);
    }
    string filePath = _dir + "ByteSliceData";
    if (filePath.at(1) == filePath.at(2) && filePath.at(1) == '/') {
        filePath.erase(1, 1);
    }
    BufferedFileWriterPtr fileWrite(new BufferedFileWriter());
    ASSERT_EQ(FSEC_OK, fileWrite->Open(filePath, filePath));
    ASSERT_EQ(FSEC_OK, writer.Dump(fileWrite));
    ASSERT_EQ(FSEC_OK, fileWrite->Close());
    fileWrite.reset();

    BufferedFileReaderPtr fileReader(new BufferedFileReader());
    ASSERT_EQ(FSEC_OK, fileReader->Open(filePath));
    char* buffer = new char[totalBytes + 1]; // totalBytes may be 0
    ssize_t ret;
    ret = fileReader->Read((void*)buffer, totalBytes, ReadOption()).GetOrThrow();
    ASSERT_EQ(totalBytes, ret);
    ASSERT_EQ((int)0, memcmp(answerStream.str().c_str(), buffer, totalBytes));
    ASSERT_EQ(FSEC_OK, fileReader->Close());
    delete[] buffer;
}

void ByteSliceWriterTest::CleanFiles()
{
    string str = "rm -rf ";
    str += _dir;
    ASSERT_GE(system(str.c_str()), 0);
}
}} // namespace indexlib::common
