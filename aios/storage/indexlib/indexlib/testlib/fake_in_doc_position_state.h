#ifndef __INDEXLIB_FAKE_IN_DOC_POSITION_STATE_H
#define __INDEXLIB_FAKE_IN_DOC_POSITION_STATE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/InDocPositionState.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_in_doc_position_iterator.h"
#include "indexlib/testlib/fake_posting_iterator.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib { namespace testlib {

class FakeInDocPositionState : public index::InDocPositionState
{
public:
    FakeInDocPositionState();
    ~FakeInDocPositionState();

public:
    void init(const FakePostingIterator::Posting& posting)
    {
        _posting = posting;
        SetTermFreq(posting.occArray.size());
    }
    void SetOwner(util::ObjectPool<FakeInDocPositionState>* owner) { _owner = owner; }

    virtual index::InDocPositionIterator* CreateInDocIterator() const
    {
        return new FakeInDocPositionIterator(_posting.occArray, _posting.fieldBitArray, _posting.sectionIdArray);
    }

    virtual index::InDocPositionState* Clone() const { return new FakeInDocPositionState(*this); }

    virtual void Free()
    {
        if (_owner) {
            _owner->Free(this);
        }
    }

private:
    FakePostingIterator::Posting _posting;
    util::ObjectPool<FakeInDocPositionState>* _owner;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeInDocPositionState);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_IN_DOC_POSITION_STATE_H
