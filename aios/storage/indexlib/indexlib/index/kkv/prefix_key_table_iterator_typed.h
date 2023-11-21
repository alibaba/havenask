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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/prefix_key_table_iterator_base.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ValueType, typename IteratorType>
class PrefixKeyTableIteratorTyped : public PrefixKeyTableIteratorBase<ValueType>
{
public:
    PrefixKeyTableIteratorTyped(IteratorType* iter) : mIter(NULL)
    {
        assert(iter);
        mIter = iter;
    }

    ~PrefixKeyTableIteratorTyped() { DELETE_AND_SET_NULL(mIter); }

public:
    bool IsValid() const override final { return mIter->IsValid(); }

    void MoveToNext() override final { return mIter->MoveToNext(); }

    void SortByKey() override final
    {
        IE_LOG(INFO, "PrefixKeyTableIterator Begin Sort.");
        mIter->SortByKey();
        IE_LOG(INFO, "PrefixKeyTableIterator End Sort.");
    }

    size_t Size() const override final { return mIter->Size(); }

    void Reset() override final { return mIter->Reset(); }

protected:
    IteratorType* mIter;

private:
    IE_LOG_DECLARE();
};
//////////////////////////
IE_LOG_SETUP_TEMPLATE2(index, PrefixKeyTableIteratorTyped);
}} // namespace indexlib::index
