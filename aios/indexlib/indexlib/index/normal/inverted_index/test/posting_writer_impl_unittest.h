#ifndef __INDEXLIB_POSTINGWRITERIMPLTEST_H
#define __INDEXLIB_POSTINGWRITERIMPLTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_encoder.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/test/posting_writer_helper.h"

IE_NAMESPACE_BEGIN(index);

class FakeSectionAttributeReader : public index::SectionAttributeReaderImpl
{
public:
    FakeSectionAttributeReader()
    {
        section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
        section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
        section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];

        for (uint32_t i = 0; i <MAX_SECTION_COUNT_PER_DOC; ++i)
        {
            lengths[i] = 2000;
            fids[i] = 0;
            weights[i] = 0;
        }
        common::SectionAttributeEncoder::Encode(lengths, fids, weights, 
                MAX_SECTION_COUNT_PER_DOC, mBuffer, sizeof(mBuffer));
    }

    virtual int32_t Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const
    {
        memcpy(buffer, mBuffer, bufLen);
        return 0;
    }

private:
    uint8_t mBuffer[MAX_SECTION_COUNT_PER_DOC * 5];
};


class PostingWriterImplTest : public INDEXLIB_TESTBASE {
public:
    PostingWriterImplTest();
    ~PostingWriterImplTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestPostingWriterWithoutPositionList();
    void TestPostingWriterWithPositionList();
    void TestPostingWriterWithoutTFList();
    void TestPostingWriterWithTFBitmap();
    void TestPostingWriterWithoutDocPayload();
    void TestPostingWriterWithoutPositionPayload();
    void TestPostingWriterWithFieldMap();
    void TestCaseForGetDictInlinePostingValue();
    void TestCaseForInMemPostingDecoder();
    void TestCaseForPostingFormat();

private:
    void Check(PostingFormatOption& postingFormatOption,
               Answer& answer, index::BufferedPostingIterator& iter,
               uint8_t compressMode);
    void CheckDocList(DocListFormatOption docListOption,
                      Answer& answer, size_t answerCursor,
                      index::BufferedPostingIterator& iter);
    void CheckPositionList(
            PositionListFormatOption positionListOption,
            index::BufferedPostingIterator& iter,
            AnswerInDoc& answerInDoc);

    void TestDump(uint32_t docCount, optionflag_t optionFlag,
                  IndexType indexType);
private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    std::string mRootDir;
    std::string mFile;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithoutPositionList);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithPositionList);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithoutTFList);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithTFBitmap);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithoutDocPayload);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithoutPositionPayload);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestPostingWriterWithFieldMap);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestCaseForGetDictInlinePostingValue);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestCaseForInMemPostingDecoder);
INDEXLIB_UNIT_TEST_CASE(PostingWriterImplTest, TestCaseForPostingFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGWRITERIMPLTEST_H
