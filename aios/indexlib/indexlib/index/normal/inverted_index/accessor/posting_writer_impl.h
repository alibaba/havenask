#ifndef __INDEXLIB_POSITION_WRITER_IMPL_H
#define __INDEXLIB_POSITION_WRITER_IMPL_H

#include <tr1/memory>
#include "indexlib/common_define.h"

#include "indexlib/indexlib.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/util/simple_pool.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"

IE_NAMESPACE_BEGIN(index);

struct SingleDocInfo
{
    tf_t tf;
    fieldmap_t fieldMap;
};

union DocListUnionStruct
{
    uint64_t dictInlineValue;
    SingleDocInfo singleDocInfo;
};

enum DocListType
{
    DLT_SINGLE_DOC_INFO = 0,
    DLT_DOC_LIST_ENCODER = 1,
    DLT_DICT_INLINE_VALUE = 2
};

class PostingWriterImpl : public index::PostingWriter
{
public:
    PostingWriterImpl(index::PostingWriterResource* writerResource,
                      bool disableDictInline = false)
        : mDocListEncoder(NULL)
        , mPositionListEncoder(NULL)
        , mWriterResource(writerResource)
        , mTermPayload(0)
    {
        assert(mWriterResource->byteSlicePool);
        if (mWriterResource->postingFormatOption.HasPositionList())
        {
            mPositionListEncoder =
                IE_POOL_NEW_CLASS(mWriterResource->byteSlicePool, PositionListEncoder,
                        mWriterResource->postingFormatOption.GetPosListFormatOption(),
                        mWriterResource->byteSlicePool, mWriterResource->bufferPool,
                        mWriterResource->postingFormat ?
                        mWriterResource->postingFormat->GetPositionListFormat():NULL);
        }
        if (disableDictInline ||
            mWriterResource->postingFormatOption.HasPositionList() ||
            mWriterResource->postingFormatOption.HasTfBitmap())
        {
            mDocListEncoder =
                IE_POOL_NEW_CLASS(mWriterResource->byteSlicePool, DocListEncoder,
                        mWriterResource->postingFormatOption.GetDocListFormatOption(),
                        mWriterResource->simplePool,
                        mWriterResource->byteSlicePool,
                        mWriterResource->bufferPool, 
                        mWriterResource->postingFormat ?
                        mWriterResource->postingFormat->GetDocListFormat() : NULL,
                        mWriterResource->postingFormatOption.GetDocListCompressMode());
            mDocListType = DLT_DOC_LIST_ENCODER;
        }
        else
        {
            mDocListUnion.singleDocInfo.fieldMap = 0;
            mDocListUnion.singleDocInfo.tf = 0;
            mDocListType = DLT_SINGLE_DOC_INFO;
        }
    }

    PostingWriterImpl(const PostingWriterImpl& postingWriterImpl) = delete;
    ~PostingWriterImpl()
    {
        if (mPositionListEncoder)
        {
            mPositionListEncoder->~PositionListEncoder();
            mPositionListEncoder = NULL;
        }

        if (mDocListEncoder)
        {
            mDocListEncoder->~DocListEncoder();
            mDocListEncoder = NULL;
        }
    }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload, int32_t fieldIdxInPack);

    void EndDocument(docid_t docId, docpayload_t docPayload);
    void EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap);

    void EndSegment();

    void Dump(const file_system::FileWriterPtr& file);
    uint32_t GetDumpLength() const;

    // termpayload need for temp store
    termpayload_t GetTermPayload() const { return mTermPayload; }

    void SetTermPayload(termpayload_t payload) { mTermPayload = payload; }

    uint32_t GetDF() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetDF();
        case DLT_DICT_INLINE_VALUE:
            return 1;
        default:
            assert(false);
            return 0;
        }
    }

    uint32_t GetTotalTF() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return mDocListUnion.singleDocInfo.tf;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetTotalTF();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue)
                .GetTermFreq();
        default:
            assert(false);
            return 0;
        }
    }

    tf_t GetCurrentTF() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return mDocListUnion.singleDocInfo.tf;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetCurrentTF();
        case DLT_DICT_INLINE_VALUE:
            return 0;
        default:
            assert(false);
            return 0;
        }
    }

    /* for only tf without position list */
    void SetCurrentTF(tf_t tf)
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            mDocListUnion.singleDocInfo.tf = tf;
            break;
        case DLT_DOC_LIST_ENCODER:
            mDocListEncoder->SetCurrentTF(tf);
            break;
        case DLT_DICT_INLINE_VALUE:
        {
            DictInlineFormatter formatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue);
            UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(),
                    formatter.GetDocId(), formatter.GetDocPayload());
            mDocListEncoder->SetCurrentTF(tf);
            break;
        }
        default:
            assert(false);
        }
    }

    // get from encoder
    bool NotExistInCurrentDoc() const { return GetCurrentTF() == 0; }

    const util::ByteSliceList* GetPositionList() const;

    index::PositionBitmapWriter* GetPositionBitmapWriter() const
    {
        if (mDocListEncoder)
        {
            return mDocListEncoder->GetPositionBitmapWriter();
        }
        return NULL;
    }

    fieldmap_t GetLastFieldMap() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetLastFieldMap();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue)
                .GetFieldMap();
        default:
            assert(false);
            return 0;
        }
    }

    void SetFieldMap(fieldmap_t fieldMap)
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            mDocListUnion.singleDocInfo.fieldMap = fieldMap;
            break;
        case DLT_DOC_LIST_ENCODER:
            mDocListEncoder->SetFieldMap(fieldMap);
            break;
        case DLT_DICT_INLINE_VALUE:
        {
            DictInlineFormatter formatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue);
            UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(),
                    formatter.GetDocId(), formatter.GetDocPayload());
            mDocListEncoder->SetFieldMap(fieldMap);
            break;
        }
        default:
            assert(false);
        }
    }

    InMemPostingDecoder* CreateInMemPostingDecoder(autil::mem_pool::Pool * sessionPool) const;

    docid_t GetLastDocId() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetLastDocId();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue)
                .GetDocId();
        default:
            assert(false);
            return INVALID_DOCID;
        }
    }

    docpayload_t GetLastDocPayload() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return 0;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetLastDocPayload();
        case DLT_DICT_INLINE_VALUE:
            return DictInlineFormatter(
                    mWriterResource->postingFormatOption, mDocListUnion.dictInlineValue)
                .GetDocPayload();
        default:
            assert(false);
            return INVALID_DOC_PAYLOAD;
        }
    }

    bool GetDictInlinePostingValue(uint64_t & inlinePostingValue);

public:
    // for test
    fieldmap_t GetFieldMap() const
    {
        switch (mDocListType)
        {
        case DLT_SINGLE_DOC_INFO:
            return mDocListUnion.singleDocInfo.fieldMap;
        case DLT_DOC_LIST_ENCODER:
            return mDocListEncoder->GetFieldMap();
        case DLT_DICT_INLINE_VALUE:
            return 0;
        default:
            assert(false);
            return 0;
        }
    }

    DocListEncoder* GetDocListEncoder() const { return mDocListEncoder; }

private:
    void UseDocListEncoder(
            tf_t tf, fieldmap_t fieldmap, docid_t docId, docpayload_t docPayload);

private:
    DocListUnionStruct volatile mDocListUnion;
    DocListEncoder* volatile mDocListEncoder;
    // pointer: PositionListEncoder may not exist
    PositionListEncoder* mPositionListEncoder;
    index::PostingWriterResource* mWriterResource;
    termpayload_t mTermPayload;
    DocListType volatile mDocListType;

private:
    friend class PostingWriterImplTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PostingWriterImpl> PostingWriterImplPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_WRITER_IMPL_H
