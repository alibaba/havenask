#ifndef ISEARCH_FAKEINDOCPOSITIONSTATE_H
#define ISEARCH_FAKEINDOCPOSITIONSTATE_H

#include <ha3/index/index.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/util/object_pool.h>

IE_NAMESPACE_BEGIN(index);

class FakeInDocPositionState : public InDocPositionState {
public:
    FakeInDocPositionState();
    virtual ~FakeInDocPositionState() {
    }
public:
    void init(const FakeTextIndexReader::Posting &posting)
    {
        _posting = posting;
        SetTermFreq(posting.occArray.size());
    }
public:
    void SetOwner(IE_NAMESPACE(util)::ObjectPool<FakeInDocPositionState> *owner) {
        _owner = owner;
    }

    virtual InDocPositionIterator* CreateInDocIterator() const;

    virtual InDocPositionState* Clone() const ;

    virtual void Free() {
        if (_owner) {
            _owner->Free(this);
        }
    }

private:
    FakeTextIndexReader::Posting _posting;
    IE_NAMESPACE(util)::ObjectPool<FakeInDocPositionState> *_owner;
private:
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);
#endif //ISEARCH_FAKEINDOCPOSITIONSTATE_H
