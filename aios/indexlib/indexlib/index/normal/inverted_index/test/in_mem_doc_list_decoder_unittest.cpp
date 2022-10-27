#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/Pool.h>

#include "indexlib/index/normal/inverted_index/test/in_mem_doc_list_decoder_unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDocListDecoderTest);

InMemDocListDecoderTest::InMemDocListDecoderTest()
{
    mDocListEncoder = NULL;
    mDocListDecoder = NULL;
}

InMemDocListDecoderTest::~InMemDocListDecoderTest()
{
}

void InMemDocListDecoderTest::CaseSetUp()
{
    mBufferPool = new RecyclePool(&mAllocator, 10240);
    mByteSlicePool = new Pool(&mAllocator, 10240);

    DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
    mDocListEncoder = new DocListEncoder(docListFormatOption, &mSimplePool,
            mByteSlicePool, mBufferPool);

    for (size_t i = 0; i < 128; ++i)
    {
        mDocListEncoder->EndDocument(i, 0);
    }
    for (size_t i = 128; i < 256; ++i)
    {
        mDocListEncoder->EndDocument(i * 2, 0);
    }
    mDocListEncoder->EndDocument(1000, 0);
}

void InMemDocListDecoderTest::CaseTearDown()
{
    mDocListEncoderPtr.reset();
    mDocListFormatPtr.reset();

    delete mDocListEncoder;
// TODO: Pool'll release it
//    delete mDocListDecoder;

    delete mBufferPool;
    delete mByteSlicePool;
}

void InMemDocListDecoderTest::TestDecodeDocBufferWithoutSkipList()
{
    DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
    DocListFormat docListFormat(docListFormatOption);
    BufferedByteSlice* postingBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool,
            BufferedByteSlice, mByteSlicePool, mBufferPool);
    postingBuffer->Init(&docListFormat);

    docid_t doc1 = 1;
    docid_t doc2 = 10;
    postingBuffer->PushBack(0, doc1);
    postingBuffer->EndPushBack();
    postingBuffer->PushBack(0, doc2 - doc1);
    postingBuffer->EndPushBack();
    
    InMemDocListDecoder docListDecoder(mByteSlicePool, false);
    docListDecoder.Init(2, NULL, postingBuffer);
    
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[1024];
    INDEXLIB_TEST_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer,
                    firstDocId, lastDocId, currentTTF));
    
    INDEXLIB_TEST_EQUAL(doc1, firstDocId);
    INDEXLIB_TEST_EQUAL(doc2, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(doc1, docBuffer[0]);
    INDEXLIB_TEST_EQUAL(doc2 - doc1, docBuffer[1]);

    INDEXLIB_TEST_TRUE(!docListDecoder.DecodeDocBuffer(doc2, docBuffer,
                    firstDocId, lastDocId, currentTTF));
}

void InMemDocListDecoderTest::TestDecodeDocBufferWithoutSkipListAndWithDocByteSlice()
{
    DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
    DocListFormat docListFormat(docListFormatOption);

    BufferedByteSlice* postingBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool,
            BufferedByteSlice, mByteSlicePool, mBufferPool);
    postingBuffer->Init(&docListFormat);

    for (docid_t i = 0; i < 128; ++i)
    {
        postingBuffer->PushBack(0, i);
        postingBuffer->EndPushBack();
    }
    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    InMemDocListDecoder docListDecoder(mByteSlicePool, false);
    docListDecoder.Init(2, NULL, postingBuffer);
    
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[1024];
    INDEXLIB_TEST_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer,
                    firstDocId, lastDocId, currentTTF));
    
    INDEXLIB_TEST_EQUAL(0, firstDocId);
    INDEXLIB_TEST_EQUAL(8128, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(0, docBuffer[0]);
    INDEXLIB_TEST_EQUAL(1, docBuffer[1]);
    INDEXLIB_TEST_EQUAL(2, docBuffer[2]);

    INDEXLIB_TEST_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    postingBuffer->PushBack(0, 128);
    postingBuffer->EndPushBack();

    //without skiplist, we only decode one time
    INDEXLIB_TEST_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    //without skiplist, we only decode one time
    INDEXLIB_TEST_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));
}

void InMemDocListDecoderTest::TestDecodeDocBufferWithSkipList()
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    mDocListDecoder = mDocListEncoder->GetInMemDocListDecoder(mByteSlicePool);

    INDEXLIB_TEST_TRUE(mDocListDecoder->DecodeDocBuffer(256, docBuffer,
                    firstDocId, lastDocId, currentTTF));
    
    INDEXLIB_TEST_EQUAL(256, firstDocId);
    INDEXLIB_TEST_EQUAL(510, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(129, docBuffer[0]);
    INDEXLIB_TEST_EQUAL(2, docBuffer[1]);    

    INDEXLIB_TEST_TRUE(mDocListDecoder->DecodeDocBuffer(1000, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    INDEXLIB_TEST_EQUAL(1000, firstDocId);
    INDEXLIB_TEST_EQUAL(1000, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(490, docBuffer[0]);

    INDEXLIB_TEST_TRUE(!mDocListDecoder->DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, mDocListDecoder);
}

void InMemDocListDecoderTest::TestDecodeDocBufferWithOneMoreDocByteSlice()
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    mDocListEncoder->EndDocument(1001, 0);
    mDocListEncoder->EndDocument(1002, 0);

    BufferedByteSlice* postingBuffer = mDocListEncoder->GetDocListBuffer();
    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    mDocListDecoder = mDocListEncoder->GetInMemDocListDecoder(mByteSlicePool);

    INDEXLIB_TEST_TRUE(mDocListDecoder->DecodeDocBuffer(1000, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    INDEXLIB_TEST_EQUAL(1000, firstDocId);
    INDEXLIB_TEST_EQUAL(1002, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(490, docBuffer[0]);
    INDEXLIB_TEST_EQUAL(1, docBuffer[1]);
    INDEXLIB_TEST_EQUAL(1, docBuffer[2]);

    INDEXLIB_TEST_TRUE(!mDocListDecoder->DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, mDocListDecoder);
}

void InMemDocListDecoderTest::TestDecodeDocBufferWithTwoMoreDocByteSlice()
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    BufferedByteSlice* postingBuffer = mDocListEncoder->GetDocListBuffer();

    for (docid_t i = 0; i < 300; ++i)
    {
        postingBuffer->PushBack(0, i);
        postingBuffer->EndPushBack();
        if (postingBuffer->NeedFlush())
        {
            postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);
        }
    }
    mDocListDecoder = mDocListEncoder->GetInMemDocListDecoder(mByteSlicePool);

    INDEXLIB_TEST_TRUE(mDocListDecoder->DecodeDocBuffer(1000, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    INDEXLIB_TEST_EQUAL(1000, firstDocId);
    INDEXLIB_TEST_EQUAL(9001, lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(490, docBuffer[0]);
    INDEXLIB_TEST_EQUAL(0, docBuffer[1]);
    INDEXLIB_TEST_EQUAL(1, docBuffer[2]);
    INDEXLIB_TEST_EQUAL(2, docBuffer[3]);

    INDEXLIB_TEST_TRUE(!mDocListDecoder->DecodeDocBuffer(lastDocId, docBuffer,
                    firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, mDocListDecoder);
}

void InMemDocListDecoderTest::TestDecodeCurrentTFBuffer()
{
    docid_t docids[] = {1, 10};
    tf_t tfs[] = {2, 4};
    TestDecodeWithOptionFlag(of_term_frequency, 2, docids, tfs, NULL, NULL);
}

void InMemDocListDecoderTest::TestDecodeCurrentDocPayloadBuffer()
{
    docid_t docids[] = {1, 10};
    docpayload_t docPayloads[] = {2, 4};
    TestDecodeWithOptionFlag(of_doc_payload, 2, docids, NULL, docPayloads, NULL);
}

void InMemDocListDecoderTest::TestDecodeCurrentFieldMapBuffer()
{
    docid_t docids[] = {1, 10};
    fieldmap_t fieldMaps[] = {2, 4};
    TestDecodeWithOptionFlag(of_fieldmap, 2, docids, NULL, NULL, fieldMaps);
}

void InMemDocListDecoderTest::TestDecodeAllBuffer()
{
    docid_t docids[] = {1, 10};
    tf_t tfs[] = {2, 4};
    docpayload_t docPayloads[] = {2, 4};
    fieldmap_t fieldMaps[] = {2, 4};
    TestDecodeWithOptionFlag(of_term_frequency | of_fieldmap | of_doc_payload,
                             2, docids, tfs, docPayloads, fieldMaps);
}

void InMemDocListDecoderTest::TestDecodeWithOptionFlag(
        const optionflag_t flag, size_t docNum, docid_t* docids, tf_t* tfs,
        docpayload_t* docPayloads, fieldmap_t* fieldMaps)
{
    DocListFormatOption docListFormatOption(flag);
    DocListFormat docListFormat(docListFormatOption);
    BufferedByteSlice* postingBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(mByteSlicePool,
            BufferedByteSlice, mByteSlicePool, mBufferPool);
    postingBuffer->Init(&docListFormat);

    docid_t prevDocId = 0;
    for (size_t i = 0; i < docNum; ++i)
    {
        uint8_t row = 0;
        postingBuffer->PushBack(row++, docids[i] - prevDocId);
        prevDocId = docids[i];
        if (docListFormatOption.HasTfList()) 
        {
            postingBuffer->PushBack(row++, tfs[i]); 
        }
        if (docListFormatOption.HasDocPayload()) 
        {
            postingBuffer->PushBack(row++, docPayloads[i]); 
        }
        if (docListFormatOption.HasFieldMap()) 
        {
            postingBuffer->PushBack(row++, fieldMaps[i]); 
        }
        postingBuffer->EndPushBack();
    }
    
    InMemDocListDecoder docListDecoder(mByteSlicePool, false);
    docListDecoder.Init(2, NULL, postingBuffer);
    
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[1024];
    INDEXLIB_TEST_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer,
                    firstDocId, lastDocId, currentTTF));
    
    INDEXLIB_TEST_EQUAL(docids[0], firstDocId);
    INDEXLIB_TEST_EQUAL(docids[docNum - 1], lastDocId);
    INDEXLIB_TEST_EQUAL((ttf_t)0, currentTTF);
    INDEXLIB_TEST_EQUAL(docids[0], docBuffer[0]);
    for (size_t i = 1; i < docNum; ++i)
    {
        INDEXLIB_TEST_EQUAL(docids[i] - docids[i - 1], docBuffer[i]);
    }

    if (docListFormatOption.HasTfList())
    {
        tf_t tfBuffer[1024];
        docListDecoder.DecodeCurrentTFBuffer(tfBuffer);
        for (size_t i = 0; i < docNum; ++i)
        {
            INDEXLIB_TEST_EQUAL(tfs[i], tfBuffer[i]);
        }
    }
    if (docListFormatOption.HasDocPayload())
    {
        docpayload_t docPayloadBuffer[1024];
        docListDecoder.DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
        for (size_t i = 0; i < docNum; ++i)
        {
            INDEXLIB_TEST_EQUAL(docPayloads[i], docPayloadBuffer[i]);
        }
    }
    if (docListFormatOption.HasFieldMap())
    {
        fieldmap_t fieldMapBuffer[1024];
        docListDecoder.DecodeCurrentFieldMapBuffer(fieldMapBuffer);
        for (size_t i = 0; i < docNum; ++i)
        {
            INDEXLIB_TEST_EQUAL(fieldMaps[i], fieldMapBuffer[i]);
        }
    }
}

void InMemDocListDecoderTest::TestDecode()
{
    TestDecode(1);
    TestDecode(MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1);
    TestDecode(MAX_DOC_PER_RECORD);
    TestDecode(MAX_DOC_PER_RECORD + 10);
    TestDecode(MAX_DOC_PER_RECORD * 5 + 1);
    TestDecode(MAX_DOC_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1);
}

void InMemDocListDecoderTest::TestDecodeWithFlush()
{
    TestDecode(1, true);
    TestDecode(MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1, true);
    TestDecode(MAX_DOC_PER_RECORD, true);
    TestDecode(MAX_DOC_PER_RECORD + 10, true);
    TestDecode(MAX_DOC_PER_RECORD * 5 + 1, true);
    TestDecode(MAX_DOC_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, true);
}

void InMemDocListDecoderTest::TestDecodeHasTf()
{
    TestDecode(1, false, true);
    TestDecode(MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1, false, true);
    TestDecode(MAX_DOC_PER_RECORD, false, true);
    TestDecode(MAX_DOC_PER_RECORD + 10, false, true);
    TestDecode(MAX_DOC_PER_RECORD * 5 + 1, false, true);
    TestDecode(MAX_DOC_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, false, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, false, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, false, true);
}

void InMemDocListDecoderTest::TestDecodeHasTfWithFlush()
{
    TestDecode(1, true, true);
    TestDecode(MAX_UNCOMPRESSED_DOC_LIST_SIZE + 1, true, true);
    TestDecode(MAX_DOC_PER_RECORD, true, true);
    TestDecode(MAX_DOC_PER_RECORD + 10, true, true);
    TestDecode(MAX_DOC_PER_RECORD * 5 + 1, true, true);
    TestDecode(MAX_DOC_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, true, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, true, true);
    TestDecode(MAX_DOC_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, true, true);
}

InMemDocListDecoder* InMemDocListDecoderTest::CreateDecoder(uint32_t docCount,
        bool needFlush, bool needTf)
{
    optionflag_t opFlag = NO_TERM_FREQUENCY;
    if (needTf)
    {
        opFlag |= of_term_frequency;
    }

    DocListFormatOption formatOption(opFlag);
    mDocListFormatPtr.reset(new DocListFormat(formatOption));

    mDocListEncoderPtr.reset(new DocListEncoder(formatOption, &mSimplePool, 
                    mByteSlicePool, mBufferPool, mDocListFormatPtr.get()));
    for (uint32_t i = 0; i < docCount; ++i)
    {
        mDocListEncoderPtr->AddPosition(0);
        mDocListEncoderPtr->AddPosition(0);
        // tf = 2
        mDocListEncoderPtr->EndDocument(i, i * 2);
    }

    if (needFlush)
    {
        mDocListEncoderPtr->Flush();
    }
    InMemDocListDecoder* decoder = mDocListEncoderPtr->GetInMemDocListDecoder(
            mByteSlicePool);
    return decoder;
}

void InMemDocListDecoderTest::TestDecode(const uint32_t docCount, 
        bool needFlush, bool needTf)
{
    InMemDocListDecoder* decoder = CreateDecoder(docCount, needFlush, needTf);

    docid_t docBuffer[docCount];
    docpayload_t docPayload[docCount];
    tf_t tfBuffer[docCount];

    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t ttf;

    uint32_t i = 0;
    for (; i < docCount / MAX_DOC_PER_RECORD; ++i)
    {
        INDEXLIB_TEST_TRUE(decoder->DecodeDocBuffer(i * MAX_DOC_PER_RECORD, 
                        docBuffer + i * MAX_DOC_PER_RECORD, 
                        firstDocId, lastDocId, ttf));
        INDEXLIB_TEST_EQUAL((docid_t)(i * MAX_DOC_PER_RECORD), firstDocId);
        INDEXLIB_TEST_EQUAL((docid_t)((i + 1) * MAX_DOC_PER_RECORD - 1), lastDocId);
        INDEXLIB_TEST_EQUAL(i * MAX_DOC_PER_RECORD, decoder->GetSeekedDocCount());
        ttf_t expectTTF = needTf ? 2 * i * MAX_DOC_PER_RECORD : 0;
        INDEXLIB_TEST_EQUAL(expectTTF, ttf);
        if (needTf)
        {
            decoder->DecodeCurrentTFBuffer(tfBuffer + i * MAX_DOC_PER_RECORD);
        }
        decoder->DecodeCurrentDocPayloadBuffer(docPayload + i * MAX_DOC_PER_RECORD);
    }

    if (docCount % MAX_DOC_PER_RECORD > 0)
    {
        INDEXLIB_TEST_TRUE(decoder->DecodeDocBuffer(i * MAX_DOC_PER_RECORD, 
                        docBuffer + i * MAX_DOC_PER_RECORD, 
                        firstDocId, lastDocId, ttf));
        INDEXLIB_TEST_EQUAL(i * MAX_DOC_PER_RECORD, decoder->GetSeekedDocCount());
        INDEXLIB_TEST_EQUAL((docid_t)(i * MAX_DOC_PER_RECORD), firstDocId);
        INDEXLIB_TEST_EQUAL((docid_t)(docCount - 1), lastDocId);
        ttf_t expectTTF = needTf ? 2 * i * MAX_DOC_PER_RECORD : 0;
        INDEXLIB_TEST_EQUAL(expectTTF, ttf);
        if (needTf)
        {
            decoder->DecodeCurrentTFBuffer(tfBuffer + i * MAX_DOC_PER_RECORD);
        }
        decoder->DecodeCurrentDocPayloadBuffer(docPayload + i * MAX_DOC_PER_RECORD);
    }

    docid_t preDocId = 0;
    for (uint32_t i = 0; i < docCount; ++i)
    {
        INDEXLIB_TEST_EQUAL((docid_t)(i - preDocId), docBuffer[i]);
        preDocId = i;
        INDEXLIB_TEST_EQUAL((docpayload_t)(i * 2), docPayload[i]);
        if (needTf)
        {
            INDEXLIB_TEST_EQUAL((tf_t)2, tfBuffer[i]);
        }
    }

    INDEXLIB_TEST_TRUE(!decoder->DecodeDocBuffer(lastDocId + 1, 
                    docBuffer + i * MAX_DOC_PER_RECORD, 
                    firstDocId, lastDocId, ttf));
    IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, decoder);
}

IE_NAMESPACE_END(index);

