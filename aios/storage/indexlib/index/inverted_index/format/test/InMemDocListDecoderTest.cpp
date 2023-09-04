#include "indexlib/index/inverted_index/format/InMemDocListDecoder.h"

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/index/inverted_index/format/DocListEncoder.h"
#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class InMemDocListDecoderTest : public TESTBASE
{
public:
    InMemDocListDecoderTest()
    {
        _docListEncoder = nullptr;
        _docListDecoder = nullptr;
    }
    ~InMemDocListDecoderTest() {}

    void setUp() override
    {
        _bufferPool = new autil::mem_pool::RecyclePool(&_allocator, 10240);
        _byteSlicePool = new autil::mem_pool::Pool(&_allocator, 10240);

        DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
        _docListEncoder = new DocListEncoder(docListFormatOption, &_simplePool, _byteSlicePool, _bufferPool);

        for (size_t i = 0; i < 128; ++i) {
            _docListEncoder->EndDocument(i, 0);
        }
        for (size_t i = 128; i < 256; ++i) {
            _docListEncoder->EndDocument(i * 2, 0);
        }
        _docListEncoder->EndDocument(1000, 0);
    }

    void tearDown() override
    {
        _docListEncoderPtr.reset();
        _docListFormatPtr.reset();

        delete _docListEncoder;
        // TODO: Pool'll release it
        //    delete _docListDecoder;

        delete _bufferPool;
        delete _byteSlicePool;
    }

private:
    void TestDecodeWithOptionFlag(const optionflag_t flag, size_t docNum, docid_t* docids, tf_t* tfs,
                                  docpayload_t* docPayloads, fieldmap_t* fieldMaps)
    {
        DocListFormatOption docListFormatOption(flag);
        DocListFormat docListFormat(docListFormatOption);
        BufferedByteSlice* postingBuffer =
            IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, BufferedByteSlice, _byteSlicePool, _bufferPool);
        postingBuffer->Init(&docListFormat);

        docid_t prevDocId = 0;
        for (size_t i = 0; i < docNum; ++i) {
            uint8_t row = 0;
            postingBuffer->PushBack(row++, docids[i] - prevDocId);
            prevDocId = docids[i];
            if (docListFormatOption.HasTfList()) {
                postingBuffer->PushBack(row++, tfs[i]);
            }
            if (docListFormatOption.HasDocPayload()) {
                postingBuffer->PushBack(row++, docPayloads[i]);
            }
            if (docListFormatOption.HasFieldMap()) {
                postingBuffer->PushBack(row++, fieldMaps[i]);
            }
            postingBuffer->EndPushBack();
        }

        InMemDocListDecoder docListDecoder(_byteSlicePool, false);
        docListDecoder.Init(2, nullptr, postingBuffer);

        docid_t firstDocId = 0;
        docid_t lastDocId = 0;
        ttf_t currentTTF = 0;
        docid_t docBuffer[1024];
        ASSERT_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer, firstDocId, lastDocId, currentTTF));

        ASSERT_EQ(docids[0], firstDocId);
        ASSERT_EQ(docids[docNum - 1], lastDocId);
        ASSERT_EQ((ttf_t)0, currentTTF);
        ASSERT_EQ(docids[0], docBuffer[0]);
        for (size_t i = 1; i < docNum; ++i) {
            ASSERT_EQ(docids[i] - docids[i - 1], docBuffer[i]);
        }

        if (docListFormatOption.HasTfList()) {
            tf_t tfBuffer[1024];
            docListDecoder.DecodeCurrentTFBuffer(tfBuffer);
            for (size_t i = 0; i < docNum; ++i) {
                ASSERT_EQ(tfs[i], tfBuffer[i]);
            }
        }
        if (docListFormatOption.HasDocPayload()) {
            docpayload_t docPayloadBuffer[1024];
            docListDecoder.DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
            for (size_t i = 0; i < docNum; ++i) {
                ASSERT_EQ(docPayloads[i], docPayloadBuffer[i]);
            }
        }
        if (docListFormatOption.HasFieldMap()) {
            fieldmap_t fieldMapBuffer[1024];
            docListDecoder.DecodeCurrentFieldMapBuffer(fieldMapBuffer);
            for (size_t i = 0; i < docNum; ++i) {
                ASSERT_EQ(fieldMaps[i], fieldMapBuffer[i]);
            }
        }
    }

    InMemDocListDecoder* CreateDecoder(uint32_t docCount, bool needFlush, bool needTf)
    {
        optionflag_t opFlag = NO_TERM_FREQUENCY;
        if (needTf) {
            opFlag |= of_term_frequency;
        }

        DocListFormatOption formatOption(opFlag);
        _docListFormatPtr.reset(new DocListFormat(formatOption));

        _docListEncoderPtr.reset(
            new DocListEncoder(formatOption, &_simplePool, _byteSlicePool, _bufferPool, _docListFormatPtr.get()));
        for (uint32_t i = 0; i < docCount; ++i) {
            _docListEncoderPtr->AddPosition(0);
            _docListEncoderPtr->AddPosition(0);
            // tf = 2
            _docListEncoderPtr->EndDocument(i, i * 2);
        }

        if (needFlush) {
            _docListEncoderPtr->Flush();
        }
        InMemDocListDecoder* decoder = _docListEncoderPtr->GetInMemDocListDecoder(_byteSlicePool);
        return decoder;
    }

    void TestDecode(const uint32_t docCount, bool needFlush = false, bool needTf = false)
    {
        InMemDocListDecoder* decoder = CreateDecoder(docCount, needFlush, needTf);

        docid_t docBuffer[docCount];
        docpayload_t docPayload[docCount];
        tf_t tfBuffer[docCount];

        docid_t firstDocId = 0;
        docid_t lastDocId = 0;
        ttf_t ttf;

        uint32_t i = 0;
        for (; i < docCount / MAX_DOC_PER_RECORD; ++i) {
            ASSERT_TRUE(decoder->DecodeDocBuffer(i * MAX_DOC_PER_RECORD, docBuffer + i * MAX_DOC_PER_RECORD, firstDocId,
                                                 lastDocId, ttf));
            ASSERT_EQ((docid_t)(i * MAX_DOC_PER_RECORD), firstDocId);
            ASSERT_EQ((docid_t)((i + 1) * MAX_DOC_PER_RECORD - 1), lastDocId);
            ASSERT_EQ(i * MAX_DOC_PER_RECORD, decoder->GetSeekedDocCount());
            ttf_t expectTTF = needTf ? 2 * i * MAX_DOC_PER_RECORD : 0;
            ASSERT_EQ(expectTTF, ttf);
            if (needTf) {
                decoder->DecodeCurrentTFBuffer(tfBuffer + i * MAX_DOC_PER_RECORD);
            }
            decoder->DecodeCurrentDocPayloadBuffer(docPayload + i * MAX_DOC_PER_RECORD);
        }

        if (docCount % MAX_DOC_PER_RECORD > 0) {
            ASSERT_TRUE(decoder->DecodeDocBuffer(i * MAX_DOC_PER_RECORD, docBuffer + i * MAX_DOC_PER_RECORD, firstDocId,
                                                 lastDocId, ttf));
            ASSERT_EQ(i * MAX_DOC_PER_RECORD, decoder->GetSeekedDocCount());
            ASSERT_EQ((docid_t)(i * MAX_DOC_PER_RECORD), firstDocId);
            ASSERT_EQ((docid_t)(docCount - 1), lastDocId);
            ttf_t expectTTF = needTf ? 2 * i * MAX_DOC_PER_RECORD : 0;
            ASSERT_EQ(expectTTF, ttf);
            if (needTf) {
                decoder->DecodeCurrentTFBuffer(tfBuffer + i * MAX_DOC_PER_RECORD);
            }
            decoder->DecodeCurrentDocPayloadBuffer(docPayload + i * MAX_DOC_PER_RECORD);
        }

        docid_t preDocId = 0;
        for (uint32_t i = 0; i < docCount; ++i) {
            ASSERT_EQ((docid_t)(i - preDocId), docBuffer[i]);
            preDocId = i;
            ASSERT_EQ((docpayload_t)(i * 2), docPayload[i]);
            if (needTf) {
                ASSERT_EQ((tf_t)2, tfBuffer[i]);
            }
        }

        ASSERT_TRUE(
            !decoder->DecodeDocBuffer(lastDocId + 1, docBuffer + i * MAX_DOC_PER_RECORD, firstDocId, lastDocId, ttf));
        IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, decoder);
    }

    autil::mem_pool::RecyclePool* _bufferPool;
    autil::mem_pool::Pool* _byteSlicePool;
    util::SimplePool _simplePool;
    autil::mem_pool::SimpleAllocator _allocator;
    InMemDocListDecoder* _docListDecoder;
    DocListEncoder* _docListEncoder;

    std::shared_ptr<DocListFormat> _docListFormatPtr;
    std::shared_ptr<DocListEncoder> _docListEncoderPtr;
};

TEST_F(InMemDocListDecoderTest, testDecodeDocBufferWithoutSkipList)
{
    DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
    DocListFormat docListFormat(docListFormatOption);
    BufferedByteSlice* postingBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, BufferedByteSlice, _byteSlicePool, _bufferPool);
    postingBuffer->Init(&docListFormat);

    docid_t doc1 = 1;
    docid_t doc2 = 10;
    postingBuffer->PushBack(0, doc1);
    postingBuffer->EndPushBack();
    postingBuffer->PushBack(0, doc2 - doc1);
    postingBuffer->EndPushBack();

    InMemDocListDecoder docListDecoder(_byteSlicePool, false);
    docListDecoder.Init(2, nullptr, postingBuffer);

    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[1024];
    ASSERT_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(doc1, firstDocId);
    ASSERT_EQ(doc2, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(doc1, docBuffer[0]);
    ASSERT_EQ(doc2 - doc1, docBuffer[1]);

    ASSERT_TRUE(!docListDecoder.DecodeDocBuffer(doc2, docBuffer, firstDocId, lastDocId, currentTTF));
}

TEST_F(InMemDocListDecoderTest, testDecodeDocBufferWithoutSkipListAndWithDocByteSlice)
{
    DocListFormatOption docListFormatOption(OPTION_FLAG_NONE);
    DocListFormat docListFormat(docListFormatOption);

    BufferedByteSlice* postingBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(_byteSlicePool, BufferedByteSlice, _byteSlicePool, _bufferPool);
    postingBuffer->Init(&docListFormat);

    for (docid_t i = 0; i < 128; ++i) {
        postingBuffer->PushBack(0, i);
        postingBuffer->EndPushBack();
    }
    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    InMemDocListDecoder docListDecoder(_byteSlicePool, false);
    docListDecoder.Init(2, nullptr, postingBuffer);

    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[1024];
    ASSERT_TRUE(docListDecoder.DecodeDocBuffer(0, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(0, firstDocId);
    ASSERT_EQ(8128, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(0, docBuffer[0]);
    ASSERT_EQ(1, docBuffer[1]);
    ASSERT_EQ(2, docBuffer[2]);

    ASSERT_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));

    postingBuffer->PushBack(0, 128);
    postingBuffer->EndPushBack();

    // without skiplist, we only decode one time
    ASSERT_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));

    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    // without skiplist, we only decode one time
    ASSERT_TRUE(!docListDecoder.DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));
}

TEST_F(InMemDocListDecoderTest, testDecodeDocBufferWithSkipList)
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    _docListDecoder = _docListEncoder->GetInMemDocListDecoder(_byteSlicePool);

    ASSERT_TRUE(_docListDecoder->DecodeDocBuffer(256, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(256, firstDocId);
    ASSERT_EQ(510, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(129, docBuffer[0]);
    ASSERT_EQ(2, docBuffer[1]);

    ASSERT_TRUE(_docListDecoder->DecodeDocBuffer(1000, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(1000, firstDocId);
    ASSERT_EQ(1000, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(490, docBuffer[0]);

    ASSERT_TRUE(!_docListDecoder->DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _docListDecoder);
}

TEST_F(InMemDocListDecoderTest, testDecodeDocBufferWithOneMoreDocByteSlice)
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    _docListEncoder->EndDocument(1001, 0);
    _docListEncoder->EndDocument(1002, 0);

    BufferedByteSlice* postingBuffer = _docListEncoder->TEST_GetDocListBuffer();
    postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);

    _docListDecoder = _docListEncoder->GetInMemDocListDecoder(_byteSlicePool);

    ASSERT_TRUE(_docListDecoder->DecodeDocBuffer(1000, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(1000, firstDocId);
    ASSERT_EQ(1002, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(490, docBuffer[0]);
    ASSERT_EQ(1, docBuffer[1]);
    ASSERT_EQ(1, docBuffer[2]);

    ASSERT_TRUE(!_docListDecoder->DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _docListDecoder);
}

TEST_F(InMemDocListDecoderTest, testDecodeDocBufferWithTwoMoreDocByteSlice)
{
    docid_t firstDocId = 0;
    docid_t lastDocId = 0;
    ttf_t currentTTF = 0;
    docid_t docBuffer[MAX_DOC_PER_RECORD];

    BufferedByteSlice* postingBuffer = _docListEncoder->TEST_GetDocListBuffer();

    for (docid_t i = 0; i < 300; ++i) {
        postingBuffer->PushBack(0, i);
        postingBuffer->EndPushBack();
        if (postingBuffer->NeedFlush()) {
            postingBuffer->Flush(PFOR_DELTA_COMPRESS_MODE);
        }
    }
    _docListDecoder = _docListEncoder->GetInMemDocListDecoder(_byteSlicePool);

    ASSERT_TRUE(_docListDecoder->DecodeDocBuffer(1000, docBuffer, firstDocId, lastDocId, currentTTF));

    ASSERT_EQ(1000, firstDocId);
    ASSERT_EQ(9001, lastDocId);
    ASSERT_EQ((ttf_t)0, currentTTF);
    ASSERT_EQ(490, docBuffer[0]);
    ASSERT_EQ(0, docBuffer[1]);
    ASSERT_EQ(1, docBuffer[2]);
    ASSERT_EQ(2, docBuffer[3]);

    ASSERT_TRUE(!_docListDecoder->DecodeDocBuffer(lastDocId, docBuffer, firstDocId, lastDocId, currentTTF));

    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _docListDecoder);
}

TEST_F(InMemDocListDecoderTest, testDecodeCurrentTFBuffer)
{
    docid_t docids[] = {1, 10};
    tf_t tfs[] = {2, 4};
    TestDecodeWithOptionFlag(of_term_frequency, 2, docids, tfs, nullptr, nullptr);
}

TEST_F(InMemDocListDecoderTest, testDecodeCurrentDocPayloadBuffer)
{
    docid_t docids[] = {1, 10};
    docpayload_t docPayloads[] = {2, 4};
    TestDecodeWithOptionFlag(of_doc_payload, 2, docids, nullptr, docPayloads, nullptr);
}

TEST_F(InMemDocListDecoderTest, testDecodeCurrentFieldMapBuffer)
{
    docid_t docids[] = {1, 10};
    fieldmap_t fieldMaps[] = {2, 4};
    TestDecodeWithOptionFlag(of_fieldmap, 2, docids, nullptr, nullptr, fieldMaps);
}

TEST_F(InMemDocListDecoderTest, testDecodeAllBuffer)
{
    docid_t docids[] = {1, 10};
    tf_t tfs[] = {2, 4};
    docpayload_t docPayloads[] = {2, 4};
    fieldmap_t fieldMaps[] = {2, 4};
    TestDecodeWithOptionFlag(of_term_frequency | of_fieldmap | of_doc_payload, 2, docids, tfs, docPayloads, fieldMaps);
}

TEST_F(InMemDocListDecoderTest, testDecode)
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

TEST_F(InMemDocListDecoderTest, testDecodeWithFlush)
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

TEST_F(InMemDocListDecoderTest, testDecodeHasTf)
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

TEST_F(InMemDocListDecoderTest, testDecodeHasTfWithFlush)
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

} // namespace indexlib::index
