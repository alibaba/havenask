#include "indexlib/index/normal/inverted_index/test/buffered_byte_slice_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedByteSliceReaderTest);

BufferedByteSliceReaderTest::BufferedByteSliceReaderTest()
{
    mByteSlicePool = new Pool(1024);
    mBufferPool = new RecyclePool(1024);
}

BufferedByteSliceReaderTest::~BufferedByteSliceReaderTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void BufferedByteSliceReaderTest::CaseSetUp()
{
    DocListFormatOption formatOption(NO_TERM_FREQUENCY);
    mDocListFormat.reset(new DocListFormat(formatOption));
    mBufferedByteSlice.reset(new BufferedByteSlice(mByteSlicePool, mBufferPool));
    mBufferedByteSlice->Init(mDocListFormat.get());
}

void BufferedByteSliceReaderTest::CaseTearDown()
{
    mBufferedByteSlice.reset();
    mDocListFormat.reset();
}

void BufferedByteSliceReaderTest::TestSimpleProcess()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    autil::mem_pool::Pool byteSlicePool(1024);
    autil::mem_pool::RecyclePool bufferPool(1024);

    BufferedByteSlice byteSlice(&byteSlicePool, &bufferPool);
    byteSlice.Init(&docListFormat);

    docid_t docId = 1;
    DocIdValue *docIdValue = docListFormat.GetDocIdValue();
    docIdValue->SetValue(docId);
    byteSlice.PushBack(docIdValue);
    byteSlice.EndPushBack();

    BufferedByteSliceReader reader;
    reader.Open(&byteSlice);

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    size_t decodeLen;
    reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen);
    INDEXLIB_TEST_EQUAL((docid_t)1, docidBuffer[0]);
}

void BufferedByteSliceReaderTest::TestDecodeWithZeroCount()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);

    {
        // empty posting buffer
        BufferedByteSlice postingBuffer(mByteSlicePool, mBufferPool);
        postingBuffer.Init(&docListFormat);

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        INDEXLIB_TEST_TRUE(!reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    }
    {
        // only has short buffer
        BufferedByteSlice postingBuffer(mByteSlicePool, mBufferPool);
        postingBuffer.Init(&docListFormat);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();
        
        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        INDEXLIB_TEST_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }
    {
        // only has flushed byte slice
        BufferedByteSlice postingBuffer(mByteSlicePool, mBufferPool);
        postingBuffer.Init(&docListFormat);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();

        postingBuffer.Flush(SHORT_LIST_COMPRESS_MODE);

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);
        
        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        INDEXLIB_TEST_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }
    {
        // has short buffer and flushed byte slice
        BufferedByteSlice postingBuffer(mByteSlicePool, mBufferPool);
        postingBuffer.Init(&docListFormat);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();

        postingBuffer.Flush(SHORT_LIST_COMPRESS_MODE);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        INDEXLIB_TEST_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }

}

void BufferedByteSliceReaderTest::TestSeek()
{
    size_t flushSize = 5;
    BufferedByteSliceReaderPtr reader = CreateReader(33, flushSize, 
            SHORT_LIST_COMPRESS_MODE);

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    docpayload_t docPayloadBuffer[MAX_DOC_PER_RECORD];    

    size_t decodeLen;
    size_t blockSize = (sizeof(docid_t) + sizeof(docpayload_t)) * flushSize + 2;
    reader->Seek(blockSize);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mShortBufferCursor);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mLocationCursor);

    INDEXLIB_TEST_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_EQUAL(flushSize, decodeLen);    
    INDEXLIB_TEST_EQUAL((docid_t)5, docidBuffer[0]);

    reader->Seek(blockSize * 3);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mShortBufferCursor);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mLocationCursor);

    INDEXLIB_TEST_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_EQUAL(flushSize, decodeLen);    
    INDEXLIB_TEST_EQUAL((docid_t)15, docidBuffer[0]);
    INDEXLIB_TEST_EQUAL((docid_t)19, docidBuffer[4]);

    INDEXLIB_TEST_TRUE(reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_EQUAL(flushSize, decodeLen);    
    INDEXLIB_TEST_EQUAL((docid_t)30, docPayloadBuffer[0]);

    reader->Seek(blockSize * 6);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mShortBufferCursor);
    INDEXLIB_TEST_EQUAL((uint32_t)0, reader->mLocationCursor);

    INDEXLIB_TEST_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_EQUAL((size_t)3, decodeLen);    
    INDEXLIB_TEST_EQUAL((docid_t)30, docidBuffer[0]);
    INDEXLIB_TEST_EQUAL((docid_t)32, docidBuffer[2]);

    INDEXLIB_TEST_TRUE(reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_EQUAL((size_t)3, decodeLen);    
    INDEXLIB_TEST_EQUAL((docid_t)60, docPayloadBuffer[0]);

    INDEXLIB_TEST_TRUE(!reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    INDEXLIB_TEST_TRUE(!reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
}

void BufferedByteSliceReaderTest::TestDecode()
{
    TestCheck(1, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(5, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(11, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(10, 5, PFOR_DELTA_COMPRESS_MODE);
    TestCheck(513, 128, PFOR_DELTA_COMPRESS_MODE);
}

BufferedByteSliceReaderPtr BufferedByteSliceReaderTest::CreateReader(
            uint32_t docCount,
            uint32_t flushCount,
            uint8_t compressMode)
{
    docid_t docId[docCount];
    docpayload_t payload[docCount];

    for (uint32_t i = 0; i < docCount; ++i)
    {
        docId[i] = i;
        payload[i] = i * 2;
    }

    return CreateReader(docId, payload, docCount, 
                        flushCount, compressMode);
}

BufferedByteSliceReaderPtr BufferedByteSliceReaderTest::CreateReader(
        docid_t docId[],
        docpayload_t docPayload[],
        uint32_t docCount,
        uint32_t flushCount,
        uint8_t compressMode)
{
    mBufferedByteSlice.reset(new BufferedByteSlice(mByteSlicePool, mBufferPool));
    mBufferedByteSlice->Init(mDocListFormat.get());

    assert(mBufferedByteSlice);
    for (size_t i = 0; i < docCount; ++i)
    {
        mBufferedByteSlice->PushBack(0, docId[i]);
        mBufferedByteSlice->PushBack(1, docPayload[i]);
        mBufferedByteSlice->EndPushBack();
        if (mBufferedByteSlice->NeedFlush(flushCount))
        {
            mBufferedByteSlice->Flush(compressMode);
        }
    }
    BufferedByteSliceReaderPtr reader(new BufferedByteSliceReader);
    reader->Open(mBufferedByteSlice.get());
    return reader;
}

void BufferedByteSliceReaderTest::TestCheck(const uint32_t docCount, uint32_t flushCount, 
        uint8_t compressMode)
{
    BufferedByteSliceReaderPtr reader = CreateReader(docCount, flushCount, compressMode);
    CheckDecode(docCount, flushCount, reader);

    reader->Open(mBufferedByteSlice.get());
    CheckDecode(docCount, flushCount, reader);
}


void BufferedByteSliceReaderTest::CheckDecode(
        uint32_t docCount, uint32_t flushCount, BufferedByteSliceReaderPtr &reader)
{
    docid_t docId[docCount];
    docpayload_t payload[docCount];

    for (uint32_t i = 0; i < docCount; ++i)
    {
        docId[i] = i;
        payload[i] = i * 2;
    }


    docid_t docidBuffer[docCount];
    docpayload_t docPayloadBuffer[docCount];    

    size_t decodeLen;
    uint32_t i = 0;
    for (; i < docCount / flushCount; ++i)
    {
        INDEXLIB_TEST_TRUE(reader->Decode(docidBuffer + i * flushCount, 
                        flushCount, decodeLen));
        INDEXLIB_TEST_EQUAL(decodeLen, (size_t)flushCount);
        INDEXLIB_TEST_TRUE(reader->Decode(docPayloadBuffer + i * flushCount, 
                        flushCount, decodeLen));
        INDEXLIB_TEST_EQUAL(decodeLen, (size_t)flushCount);
    }

    if (docCount % flushCount > 0)
    {
        INDEXLIB_TEST_TRUE(reader->Decode(docidBuffer + i * flushCount, 
                        flushCount, decodeLen));
        INDEXLIB_TEST_EQUAL(decodeLen, (size_t)docCount % flushCount);
        INDEXLIB_TEST_TRUE(reader->Decode(docPayloadBuffer + i * flushCount, 
                        flushCount, decodeLen));
        INDEXLIB_TEST_EQUAL(decodeLen, (size_t)docCount % flushCount);
    }
    INDEXLIB_TEST_TRUE(!reader->Decode(docidBuffer + i * flushCount, 
                    flushCount, decodeLen));
    INDEXLIB_TEST_TRUE(!reader->Decode(docPayloadBuffer + i * flushCount, 
                    flushCount, decodeLen));

    for (uint32_t i = 0; i < docCount; ++i)
    {
        INDEXLIB_TEST_EQUAL(docidBuffer[i], docId[i]);
        INDEXLIB_TEST_EQUAL(docPayloadBuffer[i], payload[i]);
    }
}

IE_NAMESPACE_END(index);

