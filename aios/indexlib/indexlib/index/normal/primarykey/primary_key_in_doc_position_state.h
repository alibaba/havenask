#ifndef __INDEXLIB_PRIMARY_KEY_IN_DOC_POSITION_STATE_H
#define __INDEXLIB_PRIMARY_KEY_IN_DOC_POSITION_STATE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyInDocPositionState : public InDocPositionState
{
public:
    PrimaryKeyInDocPositionState() 
        : InDocPositionState(of_none)
    {
        SetTermFreq(1);
    }

    ~PrimaryKeyInDocPositionState() {}
public:
    InDocPositionIterator* CreateInDocIterator() const override
    {
        return NULL;
    }

    void Free() override {}
};

DEFINE_SHARED_PTR(PrimaryKeyInDocPositionState);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_IN_DOC_POSITION_STATE_H
