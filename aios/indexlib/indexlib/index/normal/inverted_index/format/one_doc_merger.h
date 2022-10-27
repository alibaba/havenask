#ifndef __INDEXLIB_ONEDOCMERGER_H
#define __INDEXLIB_ONEDOCMERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(index);

class OneDocMerger
{
public:
    OneDocMerger(const PostingFormatOption& formatOption, 
                 PostingDecoderImpl *decoder);

    ~OneDocMerger();

public:
    docid_t NextDoc();
    void Merge(docid_t newDocId, PostingWriterImpl* postingWriter);

private:
    tf_t MergePosition(docid_t newDocId,
                       PostingWriterImpl* postingWriter);

private:
    docid_t mDocIdBuf[MAX_DOC_PER_RECORD];
    tf_t mTfListBuf[MAX_DOC_PER_RECORD];
    docpayload_t mDocPayloadBuf[MAX_DOC_PER_RECORD];
    fieldmap_t mFieldMapBuffer[MAX_DOC_PER_RECORD];
    pos_t mPosBuf[MAX_POS_PER_RECORD];
    pospayload_t mPosPayloadBuf[MAX_POS_PER_RECORD];

    uint32_t mDocCountInBuf;
    uint32_t mDocBufCursor;
    docid_t mDocId;

    uint32_t mPosCountInBuf;
    uint32_t mPosBufCursor;
    uint32_t mPosIndex;

    PostingDecoderImpl *mDecoder;
    PostingFormatOption mFormatOption;

    common::ReferenceCompressIntReader<uint32_t> mBufReader;

 private:
    friend class OneDocMergerTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OneDocMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ONEDOCMERGER_H
