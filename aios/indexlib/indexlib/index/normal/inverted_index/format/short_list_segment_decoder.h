#ifndef __INDEXLIB_SHORT_LIST_SEGMENT_DECODER_H
#define __INDEXLIB_SHORT_LIST_SEGMENT_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

IE_NAMESPACE_BEGIN(index);

class ShortListSegmentDecoder : public BufferedSegmentIndexDecoder
{
public:
    ShortListSegmentDecoder(common::ByteSliceReader *docListReader,
                            uint32_t docListBegin,
                            uint8_t docCompressMode);
    ~ShortListSegmentDecoder();
public:
    bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                         docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);
    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
                                docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);

    bool DecodeCurrentTFBuffer(tf_t *tfBuffer) 
    { 
        mTfEncoder->Decode((uint32_t*)tfBuffer, MAX_DOC_PER_RECORD, *mDocListReader);
        return true; 
    }

    void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) 
    { 
        mDocPayloadEncoder->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, 
                *mDocListReader);    
    }

    void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) 
    {     
        mFieldMapEncoder->Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, *mDocListReader);
    }

private:
    bool mDecodeShortList;
    common::ByteSliceReader *mDocListReader;
    uint32_t mDocListBeginPos;
    const common::Int32Encoder* mDocEncoder;
    const common::Int32Encoder* mTfEncoder;
    const common::Int16Encoder* mDocPayloadEncoder;
    const common::Int8Encoder* mFieldMapEncoder;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShortListSegmentDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHORT_LIST_SEGMENT_DECODER_H
