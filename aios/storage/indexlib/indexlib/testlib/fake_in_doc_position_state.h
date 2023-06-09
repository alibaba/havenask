/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
