#ifndef __INDEXLIB_NORMAL_IN_DOC_STATE_H
#define __INDEXLIB_NORMAL_IN_DOC_STATE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/util/object_pool.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator_state.h"

DECLARE_REFERENCE_CLASS(index, NormalInDocPositionIterator);

IE_NAMESPACE_BEGIN(index);

class NormalInDocState : public InDocPositionIteratorState
{
public:
    NormalInDocState(uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE, 
                     const index::PositionListFormatOption& option = 
                     index::PositionListFormatOption());
    NormalInDocState(const NormalInDocState& other);
    ~NormalInDocState();

public:
    InDocPositionIterator* CreateInDocIterator() const override;

    void Free() override;

    void Clone(NormalInDocState* state) const;
    void SetOwner(util::ObjectPool<NormalInDocState> *owner)
    {
        mOwner = owner;
    }
    
protected:    
    util::ObjectPool<NormalInDocState>* mOwner;

private:
    friend class NormalInDocPositionIterator;
    friend class NormalInDocStateTest;
    friend class index::PositionListSegmentDecoder;
    friend class index::InMemPositionListDecoder;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<NormalInDocState> NormalInDocStatePtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMAL_IN_DOC_STATE_H
