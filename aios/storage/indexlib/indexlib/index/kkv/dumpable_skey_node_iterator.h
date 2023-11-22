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

class DumpableSKeyNodeIterator
{
public:
    DumpableSKeyNodeIterator() {}
    virtual ~DumpableSKeyNodeIterator() {}

public:
    virtual bool IsValid() const = 0;
    virtual void MoveToNext() = 0;
    virtual uint64_t Get(autil::StringView& value) const = 0;
    virtual regionid_t GetRegionId() const = 0;
};

DEFINE_SHARED_PTR(DumpableSKeyNodeIterator);

template <typename SKeyType, size_t SKeyAppendSize>
class DumpableSKeyNodeIteratorTyped : public DumpableSKeyNodeIterator
{
public:
    struct SKeyNodeItem {
        uint64_t pkey;
        regionid_t regionId;
        NormalOnDiskSKeyNode<SKeyType> node;
        char appendData[SKeyAppendSize]; // char is required for byte align
        SKeyNodeItem(uint64_t _pkey, regionid_t _regionId, const NormalOnDiskSKeyNode<SKeyType>& _node)
            : pkey(_pkey)
            , regionId(_regionId)
            , node(_node)
        {
        }
    };

    typedef std::deque<SKeyNodeItem, autil::mem_pool::pool_allocator<SKeyNodeItem>> SKeyNodeDeque;
    typedef std::shared_ptr<SKeyNodeDeque> SKeyNodeDequePtr;

public:
    DumpableSKeyNodeIteratorTyped(const SKeyNodeDequePtr& skeyNodeDeque) : mSKeyNodeDeque(skeyNodeDeque)
    {
        assert(skeyNodeDeque);
    }

    bool IsValid() const override final
    {
        return !mSKeyNodeDeque->empty() && mSKeyNodeDeque->front().node.IsValidNode();
    }

    void MoveToNext() override final
    {
        assert(IsValid());
        mSKeyNodeDeque->pop_front();
    }

    uint64_t Get(autil::StringView& value) const override final
    {
        assert(IsValid());
        SKeyNodeItem& item = mSKeyNodeDeque->front();
        value = {(const char*)&(item.node), sizeof(NormalOnDiskSKeyNode<SKeyType>) + SKeyAppendSize};
        return item.pkey;
    }

    regionid_t GetRegionId() const override final
    {
        assert(IsValid());
        SKeyNodeItem& item = mSKeyNodeDeque->front();
        return item.regionId;
    }

private:
    SKeyNodeDequePtr mSKeyNodeDeque;
};
}} // namespace indexlib::index
