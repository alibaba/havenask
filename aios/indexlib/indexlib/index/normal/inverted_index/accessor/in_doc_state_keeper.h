#ifndef __INDEXLIB_IN_DOC_STATE_KEEPER_H
#define __INDEXLIB_IN_DOC_STATE_KEEPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/format/position_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"

IE_NAMESPACE_BEGIN(index);

class InDocStateKeeper
{
public:
    InDocStateKeeper(NormalInDocState *state,
                     autil::mem_pool::Pool *sessionPool);

    ~InDocStateKeeper();
public:
    void MoveToDoc(ttf_t currentTTF);

    void MoveToSegment(util::ByteSliceList* posList, tf_t totalTF,
                       uint32_t posListBegin, uint8_t compressMode,
                       const index::PositionListFormatOption& option);

    // for in memory segment
    void MoveToSegment(index::InMemPositionListDecoder *posDecoder);

private:
    typedef std::vector<index::PositionListSegmentDecoder*> PosDecoderVec;

private:
    NormalInDocState *mState;
    PosDecoderVec mPosDecoders;
    autil::mem_pool::Pool *mSessionPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InDocStateKeeper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_DOC_STATE_KEEPER_H
