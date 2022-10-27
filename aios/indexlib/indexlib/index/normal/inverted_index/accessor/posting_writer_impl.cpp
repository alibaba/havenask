#include <tr1/memory>
#include <limits>

#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_dict_inline_doc_list_decoder.h"

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingWriterImpl);

void PostingWriterImpl::AddPosition(pos_t pos, pospayload_t posPayload,
                                    int32_t fieldIdxInPack)
{
    if (mDocListType == DLT_SINGLE_DOC_INFO)
    {
        if (mWriterResource->postingFormatOption.HasFieldMap())
        {
            assert(fieldIdxInPack < 8);
            assert(fieldIdxInPack >= 0);
            mDocListUnion.singleDocInfo.fieldMap |= (1 << fieldIdxInPack);
        }
        mDocListUnion.singleDocInfo.tf++;
    }
    else if (mDocListType == DLT_DICT_INLINE_VALUE)
    {
        DictInlineFormatter formatter(mWriterResource->postingFormatOption,
                mDocListUnion.dictInlineValue);

        UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(),
                          formatter.GetDocId(), formatter.GetDocPayload());
        mDocListEncoder->AddPosition(fieldIdxInPack);
    }
    else
    {
        assert(mDocListType == DLT_DOC_LIST_ENCODER);
        mDocListEncoder->AddPosition(fieldIdxInPack);
    }

    if (mPositionListEncoder)
    {
        mPositionListEncoder->AddPosition(pos, posPayload);
    }
}

void PostingWriterImpl::EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap)
{
    SetFieldMap(fieldMap);
    EndDocument(docId, docPayload);
}

void PostingWriterImpl::EndDocument(docid_t docId, docpayload_t docPayload)
{
    if (mDocListType == DLT_SINGLE_DOC_INFO)
    {
        // encode singleDocInfo
        DictInlineFormatter formatter(mWriterResource->postingFormatOption);
        formatter.SetTermPayload(mTermPayload);
        formatter.SetDocId(docId);
        formatter.SetDocPayload(docPayload);        
        formatter.SetTermFreq(mDocListUnion.singleDocInfo.tf);
        formatter.SetFieldMap(mDocListUnion.singleDocInfo.fieldMap);
        uint64_t inlinePostingValue;
        if (formatter.GetDictInlineValue(inlinePostingValue))
        {
            mDocListUnion.dictInlineValue = inlinePostingValue;
            MEMORY_BARRIER(); 
            mDocListType = DLT_DICT_INLINE_VALUE;
        }
        else
        {
            UseDocListEncoder(mDocListUnion.singleDocInfo.tf,
                    mDocListUnion.singleDocInfo.fieldMap, docId, docPayload);
        }
    }
    else if (mDocListType == DLT_DICT_INLINE_VALUE)
    {
        assert(mDocListEncoder == NULL);
        DictInlineFormatter formatter(mWriterResource->postingFormatOption,
                mDocListUnion.dictInlineValue);

        UseDocListEncoder(formatter.GetTermFreq(), formatter.GetFieldMap(),
                          formatter.GetDocId(), formatter.GetDocPayload());
        mDocListEncoder->EndDocument(docId, docPayload);
    }
    else
    {
        assert(mDocListType == DLT_DOC_LIST_ENCODER);
        mDocListEncoder->EndDocument(docId, docPayload);        
    }
    
    if (mPositionListEncoder)
    {
        mPositionListEncoder->EndDocument();
    }
}

void PostingWriterImpl::EndSegment()
{
    if (mDocListType == DLT_DICT_INLINE_VALUE)
    {
        return;
    }
    if (mDocListEncoder)
    {
        assert(mDocListType == DLT_DOC_LIST_ENCODER);
        mDocListEncoder->Flush();
    }
    if (mPositionListEncoder)
    {
        mPositionListEncoder->Flush();
    }
}

uint32_t PostingWriterImpl::GetDumpLength() const
{
    if (!mDocListEncoder)
    {
        return 0;
    }
    uint32_t length = mDocListEncoder->GetDumpLength();
    if (mPositionListEncoder)
    {
        length += mPositionListEncoder->GetDumpLength();
    }
    return length;
}

void PostingWriterImpl::Dump(const file_system::FileWriterPtr& file)
{
    if (!mDocListEncoder)
    {
        return;
    }
    mDocListEncoder->Dump(file);
    if (mPositionListEncoder)
    {
        mPositionListEncoder->Dump(file);
    }
}

const ByteSliceList* PostingWriterImpl::GetPositionList() const
{
    if (mPositionListEncoder)
    {
        return mPositionListEncoder->GetPositionList();
    }
    return NULL;
}

InMemPostingDecoder* PostingWriterImpl::CreateInMemPostingDecoder(
        Pool *sessionPool) const
{
    InMemPostingDecoder* postingDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(
            sessionPool, InMemPostingDecoder);
    InMemDocListDecoder* docListDecoder = NULL;
    if (mDocListType != DLT_DOC_LIST_ENCODER)
    {
        InMemDictInlineDocListDecoder* inMemDictInlineDecoder =
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
                    InMemDictInlineDocListDecoder, sessionPool,
                    mWriterResource->postingFormatOption.IsReferenceCompress());
        if (mDocListType == DLT_DICT_INLINE_VALUE)
        {
            inMemDictInlineDecoder->Init(mWriterResource->postingFormatOption,
                             mDocListUnion.dictInlineValue);
        }
        docListDecoder = inMemDictInlineDecoder;
    }
    else
    {
        assert(mDocListType == DLT_DOC_LIST_ENCODER);
        // Doc List -> Position List
        docListDecoder = mDocListEncoder->GetInMemDocListDecoder(sessionPool);
    }
    postingDecoder->SetDocListDecoder(docListDecoder);

    if (mPositionListEncoder != NULL)
    {
        InMemPositionListDecoder* positionListDecoder = 
            mPositionListEncoder->GetInMemPositionListDecoder(sessionPool);
        postingDecoder->SetPositionListDecoder(positionListDecoder);
    }
    return postingDecoder;
}

bool PostingWriterImpl::GetDictInlinePostingValue(uint64_t& inlinePostingValue)
{
    if (mDocListType == DLT_DOC_LIST_ENCODER)
    {
        return false;
    }
    
    if (mDocListType == DLT_DICT_INLINE_VALUE)
    {
        inlinePostingValue = mDocListUnion.dictInlineValue;
        return true;
    }

    assert(mDocListType == DLT_SINGLE_DOC_INFO);
    INDEXLIB_FATAL_ERROR(InconsistentState,
                         "cannot return dictInlineValue when df = 0");
    return false;
}

void PostingWriterImpl::UseDocListEncoder(
        tf_t tf, fieldmap_t fieldMap, docid_t docId, docpayload_t docPayload)
{
    assert(mDocListEncoder == NULL);
    DocListEncoder* docListEncoder =
        IE_POOL_NEW_CLASS(mWriterResource->byteSlicePool, DocListEncoder,
                          mWriterResource->postingFormatOption.GetDocListFormatOption(),
                          mWriterResource->simplePool, mWriterResource->byteSlicePool,
                          mWriterResource->bufferPool, 
                          mWriterResource->postingFormat ?
                          mWriterResource->postingFormat->GetDocListFormat() : NULL,
                          mWriterResource->postingFormatOption.GetDocListCompressMode());
    assert(docListEncoder);
    docListEncoder->SetCurrentTF(tf);
    docListEncoder->SetFieldMap(fieldMap);
    docListEncoder->EndDocument(docId, docPayload);
    mDocListEncoder = docListEncoder;
    MEMORY_BARRIER(); 
    mDocListType = DLT_DOC_LIST_ENCODER;            
}


IE_NAMESPACE_END(index);

