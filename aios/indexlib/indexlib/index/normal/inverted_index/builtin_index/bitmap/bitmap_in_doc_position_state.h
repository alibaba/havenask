#ifndef __INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_STATE_H
#define __INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_STATE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h"
#include "indexlib/util/object_pool.h"

IE_NAMESPACE_BEGIN(index);

class BitmapInDocPositionState : public InDocPositionState
{
public:
    BitmapInDocPositionState();
    ~BitmapInDocPositionState();

public:
    InDocPositionIterator* CreateInDocIterator() const override;
    void Clone(BitmapInDocPositionState* state) const;

    void SetDocId(docid_t docId) {mDocId = docId;}    
    docid_t GetDocId() const { return mDocId;}

    void SetOwner(util::ObjectPool<BitmapInDocPositionState> *owner) { mOwner = owner; }
    void Free() override;

protected:
    util::ObjectPool<BitmapInDocPositionState>* mOwner;
    docid_t mDocId; // local Id
    friend class BitmapInDocPositionIterator;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<BitmapInDocPositionState> BitmapInDocPositionStatePtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_TEXT_IN_DOC_POSITION_STATE_H
