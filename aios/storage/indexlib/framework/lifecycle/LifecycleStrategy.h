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
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {
class SegmentDescriptions;
class SegmentStatistics;
}; // namespace indexlibv2::framework

namespace indexlib::framework {
class LifecycleStrategy
{
public:
    virtual ~LifecycleStrategy() = default;

public:
    virtual std::vector<std::pair<segmentid_t, std::string>>
    GetSegmentLifecycles(const std::shared_ptr<indexlibv2::framework::SegmentDescriptions>& segDescriptions) = 0;
    virtual std::string CalculateLifecycle(const indexlibv2::framework::SegmentStatistics& segmentStatistic) const = 0;
};

} // namespace indexlib::framework
