#include "indexlib/index/normal/inverted_index/test/buffered_byte_slice_unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedByteSliceTest);

BufferedByteSliceTest::BufferedByteSliceTest()
{
    mByteSlicePool = new Pool(1024);
    mBufferPool = new RecyclePool(1024);
}

BufferedByteSliceTest::~BufferedByteSliceTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void BufferedByteSliceTest::CaseSetUp()
{
    DocListFormatOption formatOption(NO_TERM_FREQUENCY);
    mDocListFormat.reset(new DocListFormat(formatOption));
    mBufferedByteSlice.reset(new BufferedByteSlice(mByteSlicePool, mBufferPool));
    mBufferedByteSlice->Init(mDocListFormat.get());
}

void BufferedByteSliceTest::CaseTearDown()
{
    mBufferedByteSlice.reset();
    mDocListFormat.reset();
}

void BufferedByteSliceTest::TestDecode()
{
    mBufferedByteSlice->PushBack(0, (docid_t)1);
    mBufferedByteSlice->PushBack(1, (docpayload_t)2);
    mBufferedByteSlice->EndPushBack();

    FlushInfo flushInfo = mBufferedByteSlice->GetFlushInfo();
    INDEXLIB_TEST_TRUE(flushInfo.IsValidShortBuffer());

    mBufferedByteSlice->PushBack(0, (docid_t)2);
    mBufferedByteSlice->PushBack(1, (docpayload_t)3);
    mBufferedByteSlice->EndPushBack();

    docid_t docidBuffer[MAX_DOC_PER_RECORD];
    docpayload_t docPayloadBuffer[MAX_DOC_PER_RECORD];

    BufferedByteSliceReader reader;
    reader.Open(mBufferedByteSlice.get());

    size_t decodeLen;
    reader.Decode(docidBuffer, MAX_DOC_PER_RECORD, decodeLen);
    INDEXLIB_TEST_EQUAL((docid_t)1, docidBuffer[0]);
    INDEXLIB_TEST_EQUAL((docid_t)2, docidBuffer[1]);

    reader.Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeLen);
    INDEXLIB_TEST_EQUAL((docpayload_t)2, docPayloadBuffer[0]);
    INDEXLIB_TEST_EQUAL((docpayload_t)3, docPayloadBuffer[1]);
}

void BufferedByteSliceTest::TestFlush()
{
    mBufferedByteSlice->PushBack(0, (docid_t)1);
    mBufferedByteSlice->PushBack(1, (docpayload_t)2);
    mBufferedByteSlice->EndPushBack();

    uint32_t flushSize = sizeof(docid_t) + sizeof(uint8_t) * 2 + sizeof(docpayload_t);
    INDEXLIB_TEST_EQUAL((size_t)flushSize,
                        mBufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE));

    FlushInfo flushInfo = mBufferedByteSlice->GetFlushInfo();
    INDEXLIB_TEST_TRUE(!flushInfo.IsValidShortBuffer());
    INDEXLIB_TEST_EQUAL((uint32_t)1, flushInfo.GetFlushCount());
    INDEXLIB_TEST_EQUAL(flushSize, flushInfo.GetFlushLength());
    INDEXLIB_TEST_EQUAL((uint8_t)SHORT_LIST_COMPRESS_MODE, 
                        flushInfo.GetCompressMode());

    INDEXLIB_TEST_EQUAL(flushSize, mBufferedByteSlice->EstimateDumpSize());
    INDEXLIB_TEST_EQUAL((size_t)0, mBufferedByteSlice->GetBufferSize());    

    INDEXLIB_TEST_EQUAL((size_t)0,
                        mBufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE));
}

void BufferedByteSliceTest::TestSnapShot()
{
    const int32_t decodeLen = 5;
    const int32_t count = 11;
    for (docid_t i = 0; i < count; ++i)
    {
        mBufferedByteSlice->PushBack(0, i);
        mBufferedByteSlice->PushBack(1, (docpayload_t)(i * 2));
        mBufferedByteSlice->EndPushBack();
        if (mBufferedByteSlice->NeedFlush(decodeLen))
        {
            mBufferedByteSlice->Flush(SHORT_LIST_COMPRESS_MODE);
        }
    }

    BufferedByteSlice snapShotBufferedByteSlice(mByteSlicePool, mBufferPool);
    mBufferedByteSlice->SnapShot(&snapShotBufferedByteSlice);

    INDEXLIB_TEST_EQUAL(snapShotBufferedByteSlice.GetTotalCount(),
                        mBufferedByteSlice->GetTotalCount());
    INDEXLIB_TEST_EQUAL(snapShotBufferedByteSlice.GetByteSliceList(),
                        mBufferedByteSlice->GetByteSliceList());

    BufferedByteSliceReader reader;
    reader.Open(mBufferedByteSlice.get());

    docid_t buffer[count * 2];
    docpayload_t docPayloadBuffer[count * 2];
    size_t actualDecodeLen;
    reader.Decode(buffer, decodeLen, actualDecodeLen);
    reader.Decode(docPayloadBuffer, decodeLen, actualDecodeLen);

    reader.Decode(buffer + decodeLen, decodeLen, actualDecodeLen);
    reader.Decode(docPayloadBuffer + decodeLen, decodeLen, actualDecodeLen);

    reader.Decode(buffer + decodeLen * 2, decodeLen, 
                  actualDecodeLen);
    reader.Decode(docPayloadBuffer + decodeLen * 2, decodeLen, 
                  actualDecodeLen);

    for (docid_t i = 0; i < count; ++i)
    {
        INDEXLIB_TEST_EQUAL(i, buffer[i]);
        INDEXLIB_TEST_EQUAL(i * 2, docPayloadBuffer[i]);
    }
}

IE_NAMESPACE_END(index);

