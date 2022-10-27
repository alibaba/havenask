#ifndef __INDEXLIB_BUFFERED_SEGMENT_INDEX_DECODER_H
#define __INDEXLIB_BUFFERED_SEGMENT_INDEX_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(index);

class BufferedSegmentIndexDecoder
{
public:
    BufferedSegmentIndexDecoder() : mSkipedItemCount(0) {}
    virtual ~BufferedSegmentIndexDecoder() {}

public:
    virtual bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                 docid_t &firstDocId, docid_t &lastDocId,
                                 ttf_t &currentTTF) = 0;

    virtual bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
            docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF) = 0;

    virtual bool DecodeCurrentTFBuffer(tf_t *tfBuffer) = 0;
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) = 0;
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) = 0;

    virtual void InitSkipList(uint32_t start, uint32_t end,
                              util::ByteSliceList *postingList, df_t df,
                              CompressMode compressMode) {}

    virtual uint32_t GetSeekedDocCount() const 
    { return InnerGetSeekedDocCount(); }

    uint32_t InnerGetSeekedDocCount() const
    { return mSkipedItemCount << MAX_DOC_PER_RECORD_BIT_NUM; }

protected:
    uint32_t mSkipedItemCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedSegmentIndexDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_SEGMENT_INDEX_DECODER_H
