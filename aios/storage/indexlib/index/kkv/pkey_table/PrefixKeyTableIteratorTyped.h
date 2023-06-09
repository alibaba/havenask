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

#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorBase.h"

namespace indexlibv2::index {

template <typename ValueType, typename IteratorType>
class PrefixKeyTableIteratorTyped : public PrefixKeyTableIteratorBase<ValueType>
{
public:
    PrefixKeyTableIteratorTyped(IteratorType* iter) : _iter(nullptr)
    {
        assert(iter);
        _iter = iter;
    }

    ~PrefixKeyTableIteratorTyped() { DELETE_AND_SET_NULL(_iter); }

public:
    bool IsValid() const override final { return _iter->IsValid(); }

    void MoveToNext() override final { return _iter->MoveToNext(); }

    void SortByKey() override final
    {
        AUTIL_LOG(INFO, "PrefixKeyTableIterator Begin Sort.");
        _iter->SortByKey();
        AUTIL_LOG(INFO, "PrefixKeyTableIterator End Sort.");
    }

    size_t Size() const override final { return _iter->Size(); }

    void Reset() override final { return _iter->Reset(); }

protected:
    IteratorType* _iter;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, PrefixKeyTableIteratorTyped, T1, T2);

} // namespace indexlibv2::index
