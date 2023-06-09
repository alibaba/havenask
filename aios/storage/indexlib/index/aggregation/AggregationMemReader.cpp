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
#include "indexlib/index/aggregation/AggregationMemReader.h"

#include "indexlib/index/aggregation/IValueList.h"
#include "indexlib/index/aggregation/ReadWriteState.h"

namespace indexlibv2::index {

AggregationMemReader::AggregationMemReader(const std::shared_ptr<const ReadWriteState>& state,
                                           const std::shared_ptr<const PostingMap>& data)
    : _state(state)
    , _data(data)
{
}

AggregationMemReader::~AggregationMemReader() = default;

std::unique_ptr<IKeyIterator> AggregationMemReader::CreateKeyIterator() const { return _data->CreateKeyIterator(); }

std::unique_ptr<IValueIterator> AggregationMemReader::Lookup(uint64_t key, autil::mem_pool::PoolBase* pool) const
{
    const IValueList* list = _data->Lookup(key);
    if (!list) {
        return nullptr;
    }
    return list->CreateIterator(pool);
}

bool AggregationMemReader::GetValueMeta(uint64_t key, ValueMeta& meta) const
{
    const IValueList* list = _data->Lookup(key);
    if (!list) {
        return false;
    }
    // TODO: IValueList should provide an interface GetMeta
    meta.valueCount = list->GetTotalCount();
    return true;
}

} // namespace indexlibv2::index
