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
#pragma once

#include <deque>
#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DumpablePKeyOffsetIterator
{
public:
    struct InDequeItem {
        uint64_t pkey;
        OnDiskPKeyOffset pkeyOffset;
        uint32_t skeyCount;
        InDequeItem(uint64_t _pkey, OnDiskPKeyOffset& _pkeyOffset, uint32_t _skeyCount)
            : pkey(_pkey)
            , pkeyOffset(_pkeyOffset)
            , skeyCount(_skeyCount)
        {
        }
    };

    typedef std::deque<InDequeItem, autil::mem_pool::pool_allocator<InDequeItem>> PKeyOffsetDeque;
    typedef std::shared_ptr<PKeyOffsetDeque> PKeyOffsetDequePtr;

public:
    DumpablePKeyOffsetIterator(const PKeyOffsetDequePtr& pkeyOffsetDeque) : mPKeyOffsetDeque(pkeyOffsetDeque)
    {
        assert(pkeyOffsetDeque);
    }

    bool IsValid() const { return !mPKeyOffsetDeque->empty() && mPKeyOffsetDeque->front().pkeyOffset.IsValidOffset(); }

    void MoveToNext()
    {
        assert(IsValid());
        mPKeyOffsetDeque->pop_front();
    }

    uint64_t Get(OnDiskPKeyOffset& offset) const
    {
        assert(IsValid());
        const InDequeItem& item = mPKeyOffsetDeque->front();
        offset = item.pkeyOffset;
        return item.pkey;
    }

private:
    PKeyOffsetDequePtr mPKeyOffsetDeque;
};

DEFINE_SHARED_PTR(DumpablePKeyOffsetIterator);
}} // namespace indexlib::index
