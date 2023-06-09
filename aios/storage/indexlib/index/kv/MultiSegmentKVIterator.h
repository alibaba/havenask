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
#include <utility>

#include "indexlib/index/kv/IKVIterator.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {

class AdapterIgnoreFieldCalculator;

class MultiSegmentKVIterator
{
public:
    // 要求segments按segmentId降序排，segmentId大的排在前面（数据更新）
    MultiSegmentKVIterator(schemaid_t targetSchemaId, std::shared_ptr<config::KVIndexConfig> indexConfig,
                           std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
                           std::vector<std::shared_ptr<framework::Segment>> segments);
    virtual ~MultiSegmentKVIterator();

public:
    virtual Status Init() = 0;

    virtual bool HasNext() const = 0;
    virtual Status Next(autil::mem_pool::Pool* pool, Record& record) = 0;
    virtual std::pair<offset_t, offset_t> GetOffset() const = 0;
    virtual Status Seek(offset_t segmentIndex, offset_t offset) = 0;
    virtual void Reset() = 0;

protected:
    Status CreateIterator(int32_t idx, std::unique_ptr<IKVIterator>& iterator) const;

protected:
    schemaid_t _targetSchemaId;
    std::shared_ptr<config::KVIndexConfig> _indexConfig;
    std::shared_ptr<AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;
    std::vector<std::shared_ptr<framework::Segment>> _segments;
};

} // namespace indexlibv2::index
