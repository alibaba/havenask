#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"

#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class BufferedByteSliceTest : public TESTBASE
{
public:
    BufferedByteSliceTest()
    {
        _byteSlicePool = new autil::mem_pool::Pool(1024);
        _bufferPool = new autil::mem_pool::RecyclePool(1024);
    }
    ~BufferedByteSliceTest()
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
    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::RecyclePool* _bufferPool;
    std::shared_ptr<BufferedByteSlice> _bufferedByteSlice;
    std::shared_ptr<DocListFormat> _docListFormat;
};

TEST_F(BufferedByteSliceTest, testDecode)
{
    _bufferedByteSlice->PushBack(0, (docid_t)1);
    _bufferedByteSlice->PushBack(1, (docpayload_t)2);
    _bufferedByteSlice->EndPushBack();

    FlushInfo flushInfo = _bufferedByteSlice->GetFlushInfo();
    ASSERT_TRUE(flushInfo.IsValidShortBuffer());

    _bufferedByteSlice->PushBack(0, (docid_t)2);
    _bufferedByteSlice->PushBack(1, (docpayload_t)3);
    _bufferedByteSlice->EndPushBack();

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    docpayload_t docPayloadBuffer[MAX_DOC_PER_RECORD];

    BufferedByteSliceReader reader;
    reader.Open(_bufferedByteSlice.get());

    size_t decodeLen;
    reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen);
    ASSERT_EQ((docid_t)1, docidBuffer[0]);
    ASSERT_EQ((docid_t)2, docidBuffer[1]);

    reader.Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen);
    ASSERT_EQ((docpayload_t)2, docPayloadBuffer[0]);
    ASSERT_EQ((docpayload_t)3, docPayloadBuffer[1]);
}

TEST_F(BufferedByteSliceTest, testFlush)
{
    _bufferedByteSlice->PushBack(0, (docid_t)1);
    _bufferedByteSlice->PushBack(1, (docpayload_t)2);
    _bufferedByteSlice->EndPushBack();

    uint32_t flushSize = sizeof(docid_t) + sizeof(uint8_t) * 2 + sizeof(docpayload_t);
    ASSERT_EQ((size_t)flushSize, _bufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE));

    FlushInfo flushInfo = _bufferedByteSlice->GetFlushInfo();
    ASSERT_TRUE(!flushInfo.IsValidShortBuffer());
    ASSERT_EQ((uint32_t)1, flushInfo.GetFlushCount());
    ASSERT_EQ(flushSize, flushInfo.GetFlushLength());
    ASSERT_EQ((uint8_t)SHORT_LIST_COMPRESS_MODE, flushInfo.GetCompressMode());

    ASSERT_EQ(flushSize, _bufferedByteSlice->EstimateDumpSize());
    ASSERT_EQ((size_t)0, _bufferedByteSlice->GetBufferSize());

    ASSERT_EQ((size_t)0, _bufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE));
}

TEST_F(BufferedByteSliceTest, testSnapShot)
{
    const int32_t decodeLen = 5;
    const int32_t count = 11;
    for (docid_t i = 0; i < count; ++i) {
        _bufferedByteSlice->PushBack(0, i);
        _bufferedByteSlice->PushBack(1, (docpayload_t)(i * 2));
        _bufferedByteSlice->EndPushBack();
        if (_bufferedByteSlice->NeedFlush(decodeLen)) {
            _bufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE);
        }
    }

    BufferedByteSlice snapShotBufferedByteSlice(_byteSlicePool, _bufferPool);
    _bufferedByteSlice->SnapShot(&snapShotBufferedByteSlice);

    ASSERT_EQ(snapShotBufferedByteSlice.GetTotalCount(), _bufferedByteSlice->GetTotalCount());
    ASSERT_EQ(snapShotBufferedByteSlice.GetByteSliceList(), _bufferedByteSlice->GetByteSliceList());

    BufferedByteSliceReader reader;
    reader.Open(_bufferedByteSlice.get());

    docid_t buffer[count * 2];
    docpayload_t docPayloadBuffer[count * 2];
    size_t actualDecodeLen;
    reader.Decode(buffer, decodeLen, actualDecodeLen);
    reader.Decode(docPayloadBuffer, decodeLen, actualDecodeLen);

    reader.Decode(buffer + decodeLen, decodeLen, actualDecodeLen);
    reader.Decode(docPayloadBuffer + decodeLen, decodeLen, actualDecodeLen);

    reader.Decode(buffer + decodeLen * 2, decodeLen, actualDecodeLen);
    reader.Decode(docPayloadBuffer + decodeLen * 2, decodeLen, actualDecodeLen);

    for (docid_t i = 0; i < count; ++i) {
        ASSERT_EQ(i, buffer[i]);
        ASSERT_EQ(i * 2, docPayloadBuffer[i]);
    }
}

} // namespace indexlib::index
