#ifndef __INDEXLIB_IN_MEM_POSITION_LIST_DECODER_H
#define __INDEXLIB_IN_MEM_POSITION_LIST_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemPositionListDecoder : public PositionListSegmentDecoder
{
public:
    InMemPositionListDecoder(const PositionListFormatOption &option,
                             autil::mem_pool::Pool* sessionPool);
    ~InMemPositionListDecoder();

public:
    void Init(ttf_t totalTF, index::PairValueSkipListReader* skipListReader,
              index::BufferedByteSlice* posListBuffer);

    
    bool SkipTo(ttf_t currentTTF, index::NormalInDocState* state) override;

    
    bool LocateRecord(const index::NormalInDocState* state,
                      uint32_t& termFrequence) override;

    
    uint32_t DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen,
                          pospayload_t* payloadBuffer,
                          uint32_t payloadBufferLen) override;

private:
    index::BufferedByteSlice* mPosListBuffer;
    index::BufferedByteSliceReader mPosListReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemPositionListDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_POSITION_LIST_DECODER_H
