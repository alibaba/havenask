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
#include "indexlib/index/aggregation/ReadWriteState.h"

#include "indexlib/index/aggregation/AggregationMemReader.h"

namespace indexlibv2::index {

ReadWriteState::ReadWriteState(size_t initialKeyCount,
                               const std::shared_ptr<config::AggregationIndexConfig>& indexConfig)
{
    objPool = std::make_shared<indexlib::util::SimplePool>();
    bufferPool = std::make_shared<autil::mem_pool::UnsafePool>(1 * 1024 * 1024); // 1M chunk
    postingMap = std::make_shared<PostingMap>(objPool.get(), bufferPool.get(), indexConfig, initialKeyCount);
    if (indexConfig->SupportDelete()) {
        deleteMap = std::make_shared<PostingMap>(objPool.get(), bufferPool.get(), indexConfig->GetDeleteConfig(),
                                                 initialKeyCount);
    }
}

ReadWriteState::~ReadWriteState()
{
    deleteMap.reset();
    postingMap.reset();
    bufferPool.reset();
    objPool.reset();
}

std::shared_ptr<ISegmentReader> ReadWriteState::CreateInMemoryReader() const
{
    return std::make_shared<AggregationMemReader>(shared_from_this(), postingMap);
}

std::shared_ptr<ISegmentReader> ReadWriteState::CreateDeleteReader() const
{
    if (!deleteMap) {
        return nullptr;
    }
    return std::make_shared<AggregationMemReader>(shared_from_this(), deleteMap);
}

} // namespace indexlibv2::index
