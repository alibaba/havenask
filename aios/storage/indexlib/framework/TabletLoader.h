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
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITabletLoader.h"

namespace indexlibv2::framework {

class TabletLoader : public ITabletLoader
{
public:
    using SegmentPairs = std::vector<std::pair<std::shared_ptr<Segment>, /*need Open*/ bool>>;

    TabletLoader() = default;
    virtual ~TabletLoader() = default;
    void Init(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
              const std::shared_ptr<config::ITabletSchema>& schema,
              const std::shared_ptr<indexlib::util::MemoryReserver>& memReserver, bool isOnline) override;

    Status PreLoad(const TabletData& lastTabletData, const SegmentPairs& onDiskSegmentPairs,
                   const Version& newOnDiskVersion) override;
    size_t EvaluateCurrentMemUsed(const std::vector<Segment*>& segments) override;

protected:
    // 只有实时比增量快的场景可以调用这个函数
    // 如果是building segment，且实时增量相同的情况，building segment会被保留
    std::pair<Status, std::vector<std::shared_ptr<Segment>>>
    GetRemainSegments(const TabletData& tabletData, const std::vector<std::shared_ptr<Segment>>& diskSegmemts,
                      const Version& version) const;

private:
    virtual Status DoPreLoad(const TabletData& lastTabletData,
                             std::vector<std::shared_ptr<Segment>> newOnDiskVersionSegments,
                             const Version& newOnDiskVersion) = 0;
    virtual std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema,
                                                      const std::vector<Segment*>& segments);
    std::vector<Segment*> GetNeedOpenSegments(const SegmentPairs& onDiskSegments) const;

    static Status
    ValidatePreloadParams(const std::vector<std::shared_ptr<framework::Segment>>& newOnDiskVersionSegments,
                          const framework::Version& newOnDiskVersion);

protected:
    std::string _tabletName;
    std::shared_ptr<MemoryQuotaController> _memoryQuotaController;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlib::util::MemoryReserver> _memReserver;
    bool _isOnline = true;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
