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
#include <vector>

#include "indexlib/base/Status.h"

namespace indexlib::util {
class MemoryReserver;
}

namespace indexlibv2 {
class MemoryQuotaController;

namespace config {
class TabletSchema;
}

namespace framework {
class Segment;
class Version;
class TabletData;

class ITabletLoader
{
public:
    virtual ~ITabletLoader() = default;

    virtual void Init(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                      const std::shared_ptr<config::TabletSchema>& schema,
                      const std::shared_ptr<indexlib::util::MemoryReserver>& memReserver, bool isOnline) = 0;
    // bool in onDiskSegmentPairs means need open
    virtual Status PreLoad(const TabletData& lastTabletData,
                           const std::vector<std::pair<std::shared_ptr<Segment>, bool>>& onDiskSegmentPairs,
                           const Version& newOnDiskVersion) = 0;
    virtual std::pair<Status, std::unique_ptr<TabletData>> FinalLoad(const TabletData& currentTabletData) = 0;
    virtual size_t EvaluateCurrentMemUsed(const std::vector<framework::Segment*>& segments) = 0;
};

} // namespace framework
} // namespace indexlibv2
