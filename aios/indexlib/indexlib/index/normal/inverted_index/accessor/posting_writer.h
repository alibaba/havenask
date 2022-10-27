#ifndef __INDEXLIB_POSTING_WRITER_H
#define __INDEXLIB_POSTING_WRITER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"

DECLARE_REFERENCE_CLASS(index, PositionBitmapWriter);
DECLARE_REFERENCE_CLASS(index, InMemPostingDecoder);
DECLARE_REFERENCE_CLASS(util, SimplePool);

IE_NAMESPACE_BEGIN(index);

struct PostingWriterResource
{
public:
    PostingWriterResource(util::SimplePool* _simplePool,
                          autil::mem_pool::Pool* _byteSlicePool,
                          autil::mem_pool::RecyclePool* _bufferPool,
                          index::PostingFormatOption _postingFormatOption)
        : simplePool(_simplePool)
        , byteSlicePool(_byteSlicePool)
        , bufferPool(_bufferPool)
        , postingFormatOption(_postingFormatOption)
        , postingFormat(new index::PostingFormat(_postingFormatOption))
    {
    }
    
    util::SimplePool* simplePool;
    autil::mem_pool::Pool* byteSlicePool;
    autil::mem_pool::RecyclePool* bufferPool;
    index::PostingFormatOption postingFormatOption;
    index::PostingFormatPtr postingFormat;
};
DEFINE_SHARED_PTR(PostingWriterResource);

class PostingWriter
{
public:
    PostingWriter() {}
    virtual ~PostingWriter() {}

public:
    virtual void AddPosition(pos_t pos, pospayload_t posPayload, 
                             fieldid_t fieldId) = 0;

    virtual void EndDocument(docid_t docId, docpayload_t docPayload) = 0;
    virtual void EndDocument(docid_t docId, docpayload_t docPayload,
                             fieldmap_t fieldMap) = 0;

    virtual void EndSegment() = 0;

    virtual void Dump(const file_system::FileWriterPtr& file) = 0;
    virtual uint32_t GetDumpLength() const = 0;

    virtual uint32_t GetTotalTF() const = 0;
    virtual uint32_t GetDF() const = 0;
    virtual docpayload_t GetLastDocPayload() const { return INVALID_DOC_PAYLOAD; }
    virtual fieldmap_t GetFieldMap() const { return 0; }
    virtual const util::ByteSliceList* GetPositionList() const
    { assert(false); return NULL; }
    virtual PositionBitmapWriter* GetPositionBitmapWriter() const
    { assert(false); return NULL; }

    virtual bool NotExistInCurrentDoc() const = 0;
    virtual void SetTermPayload(termpayload_t payload) = 0;
    virtual termpayload_t GetTermPayload() const = 0;

    virtual index::InMemPostingDecoder* CreateInMemPostingDecoder(
            autil::mem_pool::Pool *sessionPool) const
    { return NULL; }

    virtual docid_t GetLastDocId() const { return INVALID_DOCID; }
    
    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue) { return false; }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_WRITER_H
