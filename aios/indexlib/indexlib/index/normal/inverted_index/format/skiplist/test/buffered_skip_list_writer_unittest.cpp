#include "indexlib/index/normal/inverted_index/format/skiplist/test/buffered_skip_list_writer_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedSkipListWriterTest);

BufferedSkipListWriterTest::BufferedSkipListWriterTest()
    : mByteSlicePool(1024)
    , mBufferPool(1024)
{
    DocListFormatOption option(OPTION_FLAG_ALL);
    mDocListSkipListFormat.reset(new DocListSkipListFormat(option));
}

BufferedSkipListWriterTest::~BufferedSkipListWriterTest()
{
}

void BufferedSkipListWriterTest::CaseSetUp()
{
    mDir = GET_TEST_DATA_PATH();
}

void BufferedSkipListWriterTest::CaseTearDown()
{
}

void BufferedSkipListWriterTest::TestSimpleProcess()
{
    BufferedSkipListWriterPtr skipListBuffer(new BufferedSkipListWriter(
                    &mByteSlicePool, &mBufferPool));
    skipListBuffer->Init(mDocListSkipListFormat.get());
    skipListBuffer->AddItem(1, 2, 3);

    INDEXLIB_TEST_EQUAL((size_t)12, skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL((size_t)0, skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL((size_t)4, skipListBuffer->EstimateDumpSize());
}

void BufferedSkipListWriterTest::TestNoCompressedFlush()
{
    BufferedSkipListWriterPtr skipListBuffer(new BufferedSkipListWriter(
                    &mByteSlicePool, &mBufferPool));
    skipListBuffer->Init(mDocListSkipListFormat.get());

    size_t itemCount = 10;
    for (size_t i = 0 ; i < itemCount; ++i)
    {
        skipListBuffer->AddItem(i * 3, i * 3 + 1, i * 3 + 2);
    }

    INDEXLIB_TEST_EQUAL(itemCount * mDocListSkipListFormat->GetTotalSize(),
                        skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL((size_t)0, 
                        skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL(itemCount * mDocListSkipListFormat->GetTotalSize() - 8,
                        skipListBuffer->EstimateDumpSize());

    ByteSliceReader reader(const_cast<ByteSliceList*>(skipListBuffer->GetByteSliceList()));

    INDEXLIB_TEST_EQUAL((uint32_t)0, reader.ReadUInt32());
    for (size_t i = 1 ; i < itemCount; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)3, reader.ReadUInt32());
    }

    INDEXLIB_TEST_EQUAL((uint32_t)1, reader.ReadUInt32());
    for (size_t i = 1 ; i < itemCount; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)3, reader.ReadUInt32());
    }
    for (size_t i = 0 ; i < itemCount; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)(i * 3 + 2), reader.ReadUInt32());
    }
}

void BufferedSkipListWriterTest::TestNoCompressedDump()
{
    BufferedSkipListWriterPtr skipListBuffer(new BufferedSkipListWriter(
                    &mByteSlicePool, &mBufferPool));
    skipListBuffer->Init(mDocListSkipListFormat.get());

    size_t itemCount = 10;
    for (size_t i = 0 ; i < itemCount; ++i)
    {
        skipListBuffer->AddItem(i * 3, i * 3 + 1, i * 3 + 2);
    }

    INDEXLIB_TEST_EQUAL(itemCount * mDocListSkipListFormat->GetTotalSize(),
                        skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL((size_t)0, 
                        skipListBuffer->FinishFlush());
    INDEXLIB_TEST_EQUAL(itemCount * mDocListSkipListFormat->GetTotalSize() - 8,
                        skipListBuffer->EstimateDumpSize());

    BufferedFileWriterPtr fileWriter(new BufferedFileWriter());
    string filePath = mDir + "/dump_file";
    fileWriter->Open(filePath);
    skipListBuffer->Dump(fileWriter);
    fileWriter->Close();

    file_system::InMemFileNodePtr fileReader(file_system::InMemFileNodeCreator::Create());
    fileReader->Open(filePath, file_system::FSOT_IN_MEM);
    fileReader->Populate();
    ByteSliceListPtr sliceList(fileReader->Read(
                    storage::FileSystemWrapper::GetFileLength(filePath), 0));

    ByteSliceReader reader(sliceList.get());
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader.ReadUInt32());
    for (size_t i = 1 ; i < itemCount; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)3, reader.ReadUInt32());
    }

    INDEXLIB_TEST_EQUAL((uint32_t)1, reader.ReadUInt32());
    for (size_t i = 1 ; i < itemCount - 1; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)3, reader.ReadUInt32());
    }
    for (size_t i = 0 ; i < itemCount - 1; ++i)
    {
        INDEXLIB_TEST_EQUAL((uint32_t)(i * 3 + 2), reader.ReadUInt32());
    }
}

void BufferedSkipListWriterTest::TestCompressedFlush()
{
    BufferedSkipListWriterPtr skipListBuffer(new BufferedSkipListWriter(
                    &mByteSlicePool, &mBufferPool));
    skipListBuffer->Init(mDocListSkipListFormat.get());

    size_t itemCount = 33;
    skipListBuffer->AddItem(0, 0, 0);
    for (size_t i = 1 ; i < itemCount; ++i)
    {
        skipListBuffer->AddItem(i * 2, i * 5, 3);
    }

    skipListBuffer->FinishFlush();

    const ByteSliceList* dataList = skipListBuffer->GetByteSliceList();
    ByteSliceReader reader(const_cast<ByteSliceList*>(dataList));
    uint32_t end = dataList->GetTotalSize();

    docid_t docIdBuf[SKIP_LIST_BUFFER_SIZE];
    tf_t ttfBuf[SKIP_LIST_BUFFER_SIZE];
    uint32_t offsetBuf[SKIP_LIST_BUFFER_SIZE];

    uint32_t docCount = 0;

    docid_t docId = 0;
    uint32_t ttf = 0;
    uint32_t offset = 0;    

    const Int32Encoder* skipListEncoder = 
        EncoderProvider::GetInstance()->GetSkipListEncoder();
    while (reader.Tell() < end)
    {
        uint32_t docNum = skipListEncoder->Decode((uint32_t*)docIdBuf, 
                sizeof(docIdBuf) / sizeof(docIdBuf[0]), reader);

        uint32_t ttfNum = skipListEncoder->Decode((uint32_t*)ttfBuf, 
                sizeof(ttfBuf) / sizeof(ttfBuf[0]), reader);

        uint32_t lenNum = skipListEncoder->Decode(offsetBuf, 
                sizeof(offsetBuf) / sizeof(offsetBuf[0]), reader);
        
        INDEXLIB_TEST_TRUE(docNum == ttfNum && ttfNum == lenNum);

        for (uint32_t i = 0; i < docNum; i++)
        {
            docId += docIdBuf[i];
            ttf += ttfBuf[i];
            offset += offsetBuf[i];
            INDEXLIB_TEST_EQUAL(docId, (docid_t)docCount * 2);
            INDEXLIB_TEST_EQUAL(ttf, docCount * 5);
            INDEXLIB_TEST_EQUAL(offset, docCount * 3);
            docCount++;
        }                
    }
}

IE_NAMESPACE_END(index);

