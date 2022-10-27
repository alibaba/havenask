
#include "indexlib/index/normal/inverted_index/accessor/in_doc_state_keeper.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h" 

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InDocStateKeeper);

InDocStateKeeper::InDocStateKeeper(NormalInDocState *state,
                                   autil::mem_pool::Pool *sessionPool) 
    : mState(state)
    , mSessionPool(sessionPool)
{
}

InDocStateKeeper::~InDocStateKeeper() 
{
    for (size_t i = 0; i < mPosDecoders.size(); ++i) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mPosDecoders[i]);
    }
    mPosDecoders.clear();
}

void InDocStateKeeper::MoveToDoc(ttf_t currentTTF)
{
    assert(!mPosDecoders.empty());
    PositionListSegmentDecoder* decoder = mPosDecoders.back();
    assert(decoder);
    decoder->SkipTo(currentTTF, mState);
}

void InDocStateKeeper::MoveToSegment(util::ByteSliceList* posList,
                                     tf_t totalTF, 
                                     uint32_t posListBegin,
                                     uint8_t compressMode, 
                                     const PositionListFormatOption& option)
{
    PositionListSegmentDecoder* posSegDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(
            mSessionPool, PositionListSegmentDecoder, option, mSessionPool);
    // push back in posSegDocoder decoder vec before Init to make sure it would be deallocate when Init throw exception
    mPosDecoders.push_back(posSegDecoder);
    posSegDecoder->Init(posList, totalTF, posListBegin, compressMode, mState);
    mState->SetPositionListSegmentDecoder(posSegDecoder);
}

void InDocStateKeeper::MoveToSegment(InMemPositionListDecoder *posDecoder)
{
    assert(posDecoder);
    mPosDecoders.push_back(posDecoder);
    mState->SetPositionListSegmentDecoder(posDecoder);
}

IE_NAMESPACE_END(index);
