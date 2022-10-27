#ifndef __INDEXLIB_IN_MEM_DOC_LIST_DECODER_H
#define __INDEXLIB_IN_MEM_DOC_LIST_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemDocListDecoder : public index::BufferedSegmentIndexDecoder
{
public:
    struct DocBufferInfo
    {
        DocBufferInfo(docid_t*& _docBuffer, docid_t& _firstDocId,
                      docid_t& _lastDocId, ttf_t& _currentTTF)
            : docBuffer(_docBuffer)
            , firstDocId(_firstDocId)
            , lastDocId(_lastDocId)
            , currentTTF(_currentTTF)
        {}

        docid_t*& docBuffer;
        docid_t& firstDocId;
        docid_t& lastDocId;
        ttf_t& currentTTF;
    };

public:
    InMemDocListDecoder(autil::mem_pool::Pool *sessionPool, 
                        bool isReferenceCompress);
    ~InMemDocListDecoder();
public:
    // skipListReader and docListBuffer must be allocated by the same pool
    void Init(df_t df, index::SkipListReader* skipListReader,
              index::BufferedByteSlice* docListBuffer);

    virtual bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                 docid_t &firstDocId, docid_t &lastDocId,
                                 ttf_t &currentTTF);

    virtual bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
            docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
    {
        return DecodeDocBuffer(startDocId, docBuffer, firstDocId, lastDocId, currentTTF);
    }

    virtual bool DecodeCurrentTFBuffer(tf_t *tfBuffer);

    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer);

    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer);

private:
    bool DecodeDocBufferWithoutSkipList(docid_t lastDocIdInPrevRecord,
            uint32_t offset, docid_t startDocId, DocBufferInfo &docBufferInfo);

private:
    autil::mem_pool::Pool* mSessionPool;
    index::SkipListReader* mSkipListReader;
    index::BufferedByteSlice* mDocListBuffer;
    index::BufferedByteSliceReader mDocListReader;
    df_t mDf;
    bool mFinishDecoded;
    bool mIsReferenceCompress;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDocListDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_DOC_LIST_DECODER_H
