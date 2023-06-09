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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/SegmentMetrics.h"

namespace indexlibv2::plain {

class MultiShardSegmentMetrics : public indexlib::framework::SegmentMetrics
{
public:
    MultiShardSegmentMetrics() {}
    ~MultiShardSegmentMetrics() {}

public:
    void AddSegmentMetrics(uint32_t shardId, const std::shared_ptr<indexlib::framework::SegmentMetrics>& segMetrics)
    {
        _shardSegmentMetrics[shardId] = segMetrics;
    }
    std::shared_ptr<indexlib::framework::SegmentMetrics> GetSegmentMetrics(uint32_t shardId)
    {
        auto iter = _shardSegmentMetrics.find(shardId);
        if (iter == _shardSegmentMetrics.end()) {
            return nullptr;
        }
        return iter->second;
    }

private:
    std::map<uint32_t, std::shared_ptr<indexlib::framework::SegmentMetrics>> _shardSegmentMetrics;
};

} // namespace indexlibv2::plain
