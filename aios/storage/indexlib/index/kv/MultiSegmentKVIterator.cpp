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
#include "indexlib/index/kv/MultiSegmentKVIterator.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/KVSegmentReaderCreator.h"

namespace indexlibv2::index {

MultiSegmentKVIterator::MultiSegmentKVIterator(schemaid_t targetSchemaId,
                                               std::shared_ptr<config::KVIndexConfig> indexConfig,
                                               std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
                                               std::vector<std::shared_ptr<framework::Segment>> segments)
    : _targetSchemaId(targetSchemaId)
    , _indexConfig(std::move(indexConfig))
    , _ignoreFieldCalculator(std::move(ignoreFieldCalculator))
    , _segments(std::move(segments))
{
}

MultiSegmentKVIterator::~MultiSegmentKVIterator() {}

Status MultiSegmentKVIterator::CreateIterator(int32_t idx, std::unique_ptr<IKVIterator>& iterator) const
{
    if (idx < 0) {
        return Status::InvalidArgs("unexpected id: %d", idx);
    }

    const int32_t segmentCount = _segments.size();
    if (idx >= segmentCount) {
        return Status::Eof();
    }

    const auto& segment = _segments[idx];
    auto [s, iter] =
        KVSegmentReaderCreator::CreateIterator(segment, _indexConfig, _targetSchemaId, _ignoreFieldCalculator, false);
    if (!s.IsOK()) {
        return s;
    }
    if (!iter) {
        return Status::InternalError("create iterator for segment %d failed", segment->GetSegmentId());
    }
    iterator = std::move(iter);
    return Status::OK();
}

} // namespace indexlibv2::index
