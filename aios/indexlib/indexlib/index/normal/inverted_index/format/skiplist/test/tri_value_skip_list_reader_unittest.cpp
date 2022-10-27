#include "indexlib/index/normal/inverted_index/format/skiplist/test/tri_value_skip_list_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(TriValueSkipListReaderTest, TestCaseForSkipToNotEqual);
INDEXLIB_UNIT_TEST_CASE(TriValueSkipListReaderTest, TestCaseForSkipToEqual);
INDEXLIB_UNIT_TEST_CASE(TriValueSkipListReaderTest, TestCaseForGetSkippedItemCount);
INDEXLIB_UNIT_TEST_CASE(TriValueSkipListReaderTest, TestCaseForGetPrevAndCurrentTTF);

TriValueSkipListReaderTest::TriValueSkipListReaderTest()
{
    mByteSliceWriter = new common::ByteSliceWriter(mByteSlicePool);
}

TriValueSkipListReaderTest::~TriValueSkipListReaderTest()
{
    delete mByteSliceWriter;
    mByteSliceWriter = NULL;
}

void TriValueSkipListReaderTest::CaseSetUp()
{
    mDir = GET_TEST_DATA_PATH();
}

void TriValueSkipListReaderTest::CaseTearDown()
{
}

void TriValueSkipListReaderTest::TestCaseForGetPrevAndCurrentTTF()
{
    InnerTestGetPrevAndCurrentTTF(32, true);
    InnerTestGetPrevAndCurrentTTF(33, true);
}

void TriValueSkipListReaderTest::InnerTestGetPrevAndCurrentTTF(
        uint32_t itemCount, bool needFlush)
{
    assert(itemCount > 1);
    SkipListReaderPtr skipListReader(PrepareReader(itemCount, needFlush));
    uint32_t docId = 0;
    uint32_t preDocId = 0;
    uint32_t offset = 0;
    uint32_t delta = 0;
    uint32_t validKey = itemCount / 2 * 2 + 1;
    ASSERT_TRUE(skipListReader->SkipTo(validKey, docId, preDocId, offset, delta));
    ASSERT_EQ((uint32_t)((itemCount / 2 - 1) * 5 + 1), skipListReader->GetPrevTTF());
    ASSERT_EQ((uint32_t)((itemCount / 2) * 5 + 1), skipListReader->GetCurrentTTF());

    uint32_t invalidKey = itemCount * 2 + 1;
    ASSERT_FALSE(skipListReader->SkipTo(invalidKey, docId, preDocId, offset, delta));
    ASSERT_EQ((uint32_t)((itemCount - 1) * 5 + 1), skipListReader->GetCurrentTTF());
}

SkipListReaderPtr TriValueSkipListReaderTest::PrepareReader(
        uint32_t itemCount, bool needFlush)
{
    TearDown();
    SetUp();

    TriValueSkipListReaderPtr reader(new TriValueSkipListReader);

    mByteSliceWriter->Reset();
    for (uint32_t i = 0; i < itemCount; ++i)
    {
        mByteSliceWriter->WriteUInt32(i);
    }
    mStart = mByteSliceWriter->GetSize();

    DocListFormatOption option(OPTION_FLAG_ALL);
    DocListSkipListFormat format(option);
    BufferedSkipListWriter writer(mByteSlicePool, mBufferPool);
    writer.Init(&format);

    writer.AddItem(1, 5, 0);
    for (uint32_t i = 1; i < itemCount; ++i)
    {
        writer.AddItem(2 * i + 1, 5 * i + 1, 3);
    }
        
    writer.FinishFlush();

    file_system::FileWriterPtr fileWriter = 
        GET_PARTITION_DIRECTORY()->CreateFileWriter("dump_file");
    writer.Dump(fileWriter);
    fileWriter->Close();

    FileReaderPtr fileReader = 
        GET_PARTITION_DIRECTORY()->CreateFileReader("dump_file", FSOT_IN_MEM);
    size_t fileLen = fileReader->GetLength();
    util::ByteSliceListPtr sliceList(fileReader->Read(fileLen, 0));
    mByteSliceWriter->Write(*sliceList, 0, fileLen);
    
    mEnd = mByteSliceWriter->GetSize();

    reader->Load(mByteSliceWriter->GetByteSliceList(), mStart, mEnd, itemCount);

    return reader;
}

IE_NAMESPACE_END(index);
