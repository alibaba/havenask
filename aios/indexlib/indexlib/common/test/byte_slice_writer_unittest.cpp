#include "indexlib/common/test/byte_slice_writer_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/buffered_file_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);

void ByteSliceWriterTest::CaseSetUp()
{
    mDir = GET_TEST_DATA_PATH();
}

void ByteSliceWriterTest::CaseTearDown()
{
}

void ByteSliceWriterTest::TestCaseForWriteInt()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    uint32_t size = 0;

    //test write byte
    uint8_t byteValue = 12;
    writer.WriteByte(byteValue);
    size += sizeof(byteValue);
    INDEXLIB_TEST_EQUAL(size, writer.GetSize());

    //test write int16_t
    int16_t int16Value = 120;
    writer.WriteInt16(int16Value);
    size += sizeof(int16_t);
    INDEXLIB_TEST_EQUAL(size, writer.GetSize());

    //test write int32_t
    int32_t int32Value = 12;
    writer.WriteInt32(int32Value);
    size += sizeof(int32_t);
    INDEXLIB_TEST_EQUAL(size, writer.GetSize());

    //test write uint32_t
    uint32_t uint32Value = 21;
    writer.WriteUInt32(uint32Value);
    size += sizeof(uint32_t);
    INDEXLIB_TEST_EQUAL(size, writer.GetSize());
}

void ByteSliceWriterTest::TestCaseForWriteManyByte()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;        
    int32_t numInts = 0;
    for (numInts = 0; numInts < 40000; numInts++)
    {
        writer.WriteByte((uint8_t)numInts);
    }
    INDEXLIB_TEST_EQUAL(numInts * sizeof(uint8_t), writer.GetSize());
    CheckList<uint8_t>(writer.GetByteSliceList(), numInts);

    IE_LOG(TRACE1, "End");
}

void ByteSliceWriterTest::TestCaseForWriteManyInt16()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;        
    int16_t numInts = 0;
    for (numInts = 0; numInts < 10000; numInts++)
    {
        writer.WriteInt16(numInts);
    }
    INDEXLIB_TEST_EQUAL(numInts * sizeof(int16_t), writer.GetSize());
    CheckList<int16_t>(writer.GetByteSliceList(), numInts);
}

void ByteSliceWriterTest::TestCaseForWriteManyInt32()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;        
    int32_t numInts = 0;
    for (numInts = 0; numInts < 10000; numInts++)
    {
        writer.WriteInt32(numInts);
    }
    INDEXLIB_TEST_EQUAL(numInts * sizeof(int32_t), writer.GetSize());
    CheckList<int32_t>(writer.GetByteSliceList(), numInts);
}

void ByteSliceWriterTest::TestCaseForWrite()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    string str(16000, 'x');
    writer.Write(str.c_str(), str.size());
    INDEXLIB_TEST_EQUAL(str.size(), writer.GetSize());
    CheckList<char>(writer.GetByteSliceList(), 16000, 'x');
}

void ByteSliceWriterTest::TestCaseForWriteByteSliceList()
{
    uint32_t totalBytes = 800;
    stringstream answerStream;
    for (uint32_t i = 0; i < totalBytes; ++i)
    {
        answerStream << char(i % 256);
    }
    ByteSliceWriter writer;
    writer.Write(answerStream.str().data(), answerStream.str().size());
    ByteSliceList* oriByteSliceList = writer.GetByteSliceList();
    uint32_t start, end;
    for (start = 0; start < totalBytes - 1; ++start)
    {
        for (end = start + 1; end < totalBytes; ++end)
        {
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
    for (uint32_t i = 0; i < totalBytes; ++i)
    {
        ss << char(i % 256);
    }
    ByteSliceWriter writer;
    writer.Write(ss.str().data(), ss.str().size());
    ByteSliceList* oriByteSliceList = writer.GetByteSliceList();
    ByteSliceWriter writer2;
    bool exceptionCatched = false;
    try
    {
        writer2.Write(*oriByteSliceList, 1, 1);
    }
    catch (OutOfRangeException& e)
    {
        exceptionCatched = true;
    }
    INDEXLIB_TEST_TRUE(exceptionCatched);

    exceptionCatched = false;
    try
    {
        writer2.Write(*oriByteSliceList, 0, totalBytes + 1);
    }
    catch (OutOfRangeException& e)
    {
        exceptionCatched = true;
    }
    INDEXLIB_TEST_TRUE(exceptionCatched);
}

void ByteSliceWriterTest::TestCaseForWriteAndWriteByte()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    writer.WriteByte('x');
    string str(16000, 'x');
    writer.Write(str.c_str(), str.size());

    INDEXLIB_TEST_EQUAL(str.size() + sizeof(int8_t), writer.GetSize());
    CheckList<char>(writer.GetByteSliceList(), 16001, 'x');
}

void ByteSliceWriterTest::TestCaseForHoldInt32()
{
    ByteSliceWriter writer;
    uint32_t size = 0;

    int32_t& valueHold = writer.HoldInt32();
    size += sizeof(int32_t);
    INDEXLIB_TEST_EQUAL(size, writer.GetSize());
    writer.WriteInt32(12);
    valueHold = 12;

    CheckList<int32_t>(writer.GetByteSliceList(), 2, 12);
}

void ByteSliceWriterTest::TestCaseForReset()
{
    IE_LOG(TRACE1, "Begin");

    ByteSliceWriter writer;
    string str(1600, 'x');
    writer.Write(str.c_str(), str.size());
    INDEXLIB_TEST_EQUAL(str.size(), writer.GetSize());

    writer.Reset();
    INDEXLIB_TEST_EQUAL((size_t)0, writer.GetSize());
}

void ByteSliceWriterTest::TestCaseForDump()
{
    TestForDump(0);
    // TestForDump(1);
    // TestForDump(ByteSliceWriter::MIN_SLICE_SIZE);
    // TestForDump(ByteSliceWriter::MIN_SLICE_SIZE + 2);
    // TestForDump(3095);
}

//slice size should <= chunk size
void ByteSliceWriterTest::TestForCreateSlice()
{
    uint32_t chunkSize = DEFAULT_CHUNK_SIZE * 1024 * 1024;
    Pool* byteSlicePool = new Pool(chunkSize);

    {
        ByteSliceWriter writer(byteSlicePool);
        size_t valueSize = 100 * 1024 * 1024;
        uint8_t value = 0;

        for (uint32_t i = 0; i < valueSize; i++)
        {
            writer.WriteByte(value);
        }
        INDEXLIB_TEST_EQUAL(valueSize, writer.GetSize());
    }

    delete byteSlicePool;
}
void ByteSliceWriterTest::CheckListInBinaryFormat(const ByteSliceList* list, const string& answer,
                             uint32_t start, uint32_t end)
{
    INDEXLIB_TEST_EQUAL(end - start, list->GetTotalSize());
    uint8_t* buffer = new uint8_t[list->GetTotalSize()];
    size_t n = 0;
    ByteSlice* slice = list->GetHead();
    while (slice)
    {
        memcpy(buffer + n, slice->data, slice->size);
        n += slice->size;
        INDEXLIB_TEST_TRUE(n <= list->GetTotalSize());
        slice = slice->next;
    }

    int ret = memcmp(buffer, answer.substr(start, end - start).data(), end - start);
    INDEXLIB_TEST_TRUE(ret == 0);
    delete[] buffer;
}

void ByteSliceWriterTest::TestForDump(uint32_t totalBytes)
{
    CleanFiles();

    ByteSliceWriter writer;
    stringstream answerStream;
    for (uint32_t i = 0; i < totalBytes; ++i)
    {
        writer.WriteByte(char(i % 256));
        answerStream << char(i % 256);
    }
    string filePath =  mDir + "ByteSliceData";
    BufferedFileWriterPtr fileWrite(new BufferedFileWriter());
    fileWrite->Open(filePath);
    writer.Dump(fileWrite);
    fileWrite->Close();
    fileWrite.reset();

    BufferedFileReaderPtr fileReader(new BufferedFileReader());
    fileReader->Open(filePath);
    char* buffer = new char[totalBytes + 1]; //totalBytes may be 0
    ssize_t ret;
    ret = fileReader->Read((void*)buffer, totalBytes);
    INDEXLIB_TEST_EQUAL(totalBytes, ret);
    INDEXLIB_TEST_EQUAL((int)0, memcmp(answerStream.str().c_str(), buffer, totalBytes));
    fileReader->Close();
    delete[] buffer;
}

void ByteSliceWriterTest::CleanFiles()
{
    string str = "rm -rf ";
    str += mDir;
    system(str.c_str());
}

IE_NAMESPACE_END(common);
