#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"

#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class BufferedByteSliceReaderTest : public TESTBASE
{
public:
    BufferedByteSliceReaderTest()
    {
        _byteSlicePool = new autil::mem_pool::Pool(1024);
        _bufferPool = new autil::mem_pool::RecyclePool(1024);
    }
    ~BufferedByteSliceReaderTest()
    {
        delete _byteSlicePool;
        delete _bufferPool;
    }

    void setUp() override
    {
        DocListFormatOption formatOption(NO_TERM_FREQUENCY);
        _docListFormat.reset(new DocListFormat(formatOption));
        _bufferedByteSlice.reset(new BufferedByteSlice(_byteSlicePool, _bufferPool));
        _bufferedByteSlice->Init(_docListFormat.get());
    }
    void tearDown() override
    {
        _bufferedByteSlice.reset();
        _docListFormat.reset();
    }

private:
    void CheckDecode(uint32_t docCount, uint32_t flushCount, std::shared_ptr<BufferedByteSliceReader>& reader)
    {
        docid_t docId[docCount];
        docpayload_t payload[docCount];

        for (uint32_t i = 0; i < docCount; ++i) {
            docId[i] = i;
            payload[i] = i * 2;
        }

        docid_t docidBuffer[docCount];
        docpayload_t docPayloadBuffer[docCount];

        size_t decodeLen;
        uint32_t i = 0;
        for (; i < docCount / flushCount; ++i) {
            ASSERT_TRUE(reader->Decode(docidBuffer + i * flushCount, flushCount, decodeLen));
            ASSERT_EQ(decodeLen, (size_t)flushCount);
            ASSERT_TRUE(reader->Decode(docPayloadBuffer + i * flushCount, flushCount, decodeLen));
            ASSERT_EQ(decodeLen, (size_t)flushCount);
        }

        if (docCount % flushCount > 0) {
            ASSERT_TRUE(reader->Decode(docidBuffer + i * flushCount, flushCount, decodeLen));
            ASSERT_EQ(decodeLen, (size_t)docCount % flushCount);
            ASSERT_TRUE(reader->Decode(docPayloadBuffer + i * flushCount, flushCount, decodeLen));
            ASSERT_EQ(decodeLen, (size_t)docCount % flushCount);
        }
        ASSERT_TRUE(!reader->Decode(docidBuffer + i * flushCount, flushCount, decodeLen));
        ASSERT_TRUE(!reader->Decode(docPayloadBuffer + i * flushCount, flushCount, decodeLen));

        for (uint32_t i = 0; i < docCount; ++i) {
            ASSERT_EQ(docidBuffer[i], docId[i]);
            ASSERT_EQ(docPayloadBuffer[i], payload[i]);
        }
    }

    std::shared_ptr<BufferedByteSliceReader> CreateReader(docid_t docId[], docpayload_t docPayload[], uint32_t docCount,
                                                          uint32_t flushCount, uint8_t compressMode)
    {
        _bufferedByteSlice.reset(new BufferedByteSlice(_byteSlicePool, _bufferPool));
        _bufferedByteSlice->Init(_docListFormat.get());

        assert(_bufferedByteSlice);
        for (size_t i = 0; i < docCount; ++i) {
            _bufferedByteSlice->PushBack(0, docId[i]);
            _bufferedByteSlice->PushBack(1, docPayload[i]);
            _bufferedByteSlice->EndPushBack();
            if (_bufferedByteSlice->NeedFlush(flushCount)) {
                _bufferedByteSlice->Flush(compressMode);
            }
        }
        std::shared_ptr<BufferedByteSliceReader> reader(new BufferedByteSliceReader);
        reader->Open(_bufferedByteSlice.get());
        return reader;
    }

    void TestCheck(const uint32_t docCount, uint32_t flushCount, uint8_t compressMode)
    {
        std::shared_ptr<BufferedByteSliceReader> reader = CreateReader(docCount, flushCount, compressMode);
        CheckDecode(docCount, flushCount, reader);

        reader->Open(_bufferedByteSlice.get());
        CheckDecode(docCount, flushCount, reader);
    }

    std::shared_ptr<BufferedByteSliceReader> CreateReader(uint32_t docCount, uint32_t flushCount, uint8_t compressMode)
    {
        docid_t docId[docCount];
        docpayload_t payload[docCount];

        for (uint32_t i = 0; i < docCount; ++i) {
            docId[i] = i;
            payload[i] = i * 2;
        }

        return CreateReader(docId, payload, docCount, flushCount, compressMode);
    }

    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::RecyclePool* _bufferPool;
    std::shared_ptr<BufferedByteSlice> _bufferedByteSlice;
    std::shared_ptr<DocListFormat> _docListFormat;
};

TEST_F(BufferedByteSliceReaderTest, testSimpleProcess)
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    autil::mem_pool::Pool byteSlicePool(1024);
    autil::mem_pool::RecyclePool bufferPool(1024);

    BufferedByteSlice byteSlice(&byteSlicePool, &bufferPool);
    byteSlice.Init(&docListFormat);

    docid_t docId = 1;
    DocIdValue* docIdValue = docListFormat.GetDocIdValue();
    docIdValue->SetValue(docId);
    byteSlice.PushBack(docIdValue);
    byteSlice.EndPushBack();

    BufferedByteSliceReader reader;
    reader.Open(&byteSlice);

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    size_t decodeLen;
    reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen);
    ASSERT_EQ((docid_t)1, docidBuffer[0]);
}

TEST_F(BufferedByteSliceReaderTest, testDecodeWithZeroCount)
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);

    {
        // empty posting buffer
        BufferedByteSlice postingBuffer(_byteSlicePool, _bufferPool);
        postingBuffer.Init(&docListFormat);

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        ASSERT_TRUE(!reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    }
    {
        // only has short buffer
        BufferedByteSlice postingBuffer(_byteSlicePool, _bufferPool);
        postingBuffer.Init(&docListFormat);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        ASSERT_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }
    {
        // only has flushed byte slice
        BufferedByteSlice postingBuffer(_byteSlicePool, _bufferPool);
        postingBuffer.Init(&docListFormat);

        postingBuffer.PushBack(0, (docid_t)1);
        postingBuffer.EndPushBack();

        postingBuffer.Flush(SHORT_LIST_COMPRESS_MODE);

        BufferedByteSliceReader reader;
        reader.Open(&postingBuffer);

        docid_t docidBuffer[MAX_DOC_PER_RECORD];
        size_t decodeLen;
        ASSERT_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }
    {
        // has short buffer and flushed byte slice
        BufferedByteSlice postingBuffer(_byteSlicePool, _bufferPool);
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
        ASSERT_TRUE(!reader.Decode(docidBuffer, 0, decodeLen));
    }
}

TEST_F(BufferedByteSliceReaderTest, testSeek)
{
    size_t flushSize = 5;
    std::shared_ptr<BufferedByteSliceReader> reader = CreateReader(33, flushSize, SHORT_LIST_COMPRESS_MODE);

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    docpayload_t docPayloadBuffer[MAX_DOC_PER_RECORD];

    size_t decodeLen;
    size_t blockSize = (sizeof(docid_t) + sizeof(docpayload_t)) * flushSize + 2;
    reader->Seek(blockSize);
    ASSERT_EQ((uint32_t)0, reader->_shortBufferCursor);
    ASSERT_EQ((uint32_t)0, reader->_locationCursor);

    ASSERT_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_EQ(flushSize, decodeLen);
    ASSERT_EQ((docid_t)5, docidBuffer[0]);

    reader->Seek(blockSize * 3);
    ASSERT_EQ((uint32_t)0, reader->_shortBufferCursor);
    ASSERT_EQ((uint32_t)0, reader->_locationCursor);

    ASSERT_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_EQ(flushSize, decodeLen);
    ASSERT_EQ((docid_t)15, docidBuffer[0]);
    ASSERT_EQ((docid_t)19, docidBuffer[4]);

    ASSERT_TRUE(reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_EQ(flushSize, decodeLen);
    ASSERT_EQ((docid_t)30, docPayloadBuffer[0]);

    reader->Seek(blockSize * 6);
    ASSERT_EQ((uint32_t)0, reader->_shortBufferCursor);
    ASSERT_EQ((uint32_t)0, reader->_locationCursor);

    ASSERT_TRUE(reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_EQ((size_t)3, decodeLen);
    ASSERT_EQ((docid_t)30, docidBuffer[0]);
    ASSERT_EQ((docid_t)32, docidBuffer[2]);

    ASSERT_TRUE(reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_EQ((size_t)3, decodeLen);
    ASSERT_EQ((docid_t)60, docPayloadBuffer[0]);

    ASSERT_TRUE(!reader->Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen));
    ASSERT_TRUE(!reader->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen));
}

TEST_F(BufferedByteSliceReaderTest, testDecode)
{
    TestCheck(1, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(5, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(11, 5, SHORT_LIST_COMPRESS_MODE);
    TestCheck(10, 5, PFOR_DELTA_COMPRESS_MODE);
    TestCheck(513, 128, PFOR_DELTA_COMPRESS_MODE);
}

} // namespace indexlib::index
