#pragma once

#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/index/inverted_index/InDocPositionState.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/ObjectPool.h"

namespace indexlib {
namespace index {
class InDocPositionIterator;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeInDocPositionState : public InDocPositionState {
public:
    FakeInDocPositionState();
    virtual ~FakeInDocPositionState() {}

public:
    void init(const FakeTextIndexReader::Posting &posting) {
        _posting = posting;
        SetTermFreq(posting.occArray.size());
    }

public:
    void SetOwner(indexlib::util::ObjectPool<FakeInDocPositionState> *owner) {
        _owner = owner;
    }

    virtual InDocPositionIterator *CreateInDocIterator() const;

    virtual InDocPositionState *Clone() const;

    virtual void Free() {
        if (_owner) {
            _owner->Free(this);
        }
    }

private:
    FakeTextIndexReader::Posting _posting;
    indexlib::util::ObjectPool<FakeInDocPositionState> *_owner;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
