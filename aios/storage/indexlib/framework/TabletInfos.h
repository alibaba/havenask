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
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/Version.h"

namespace indexlib::util {
class CounterMap;
class StateCounter;
} // namespace indexlib::util

namespace indexlibv2::framework {
class TabletMetrics;
struct VersionDeployDescription;

enum class MemoryStatus {
    OK = 0,
    REACH_MAX_RT_INDEX_SIZE = 1,
    REACH_TOTAL_MEM_LIMIT = 2,
    REACH_LOW_WATERMARK_MEM_LIMIT = 3,
    REACH_HIGH_WATERMARK_MEM_LIMIT = 4,
    UNKNOWN = 5
};

class TabletInfos
{
public:
    TabletInfos();
    ~TabletInfos();

    const indexlib::framework::TabletId& GetTabletId() const;
    const std::string& GetTabletName() const;
    const IndexRoot& GetIndexRoot() const;
    MemoryStatus GetMemoryStatus() const;
    const std::shared_ptr<indexlib::util::CounterMap>& GetCounterMap() const;
    Version GetLoadedPublishVersion() const;
    Version GetLoadedPrivateVersion() const;
    Locator GetBuildLocator() const;
    Locator GetLastCommittedLocator() const;
    Locator GetLatestLocator() const;
    int64_t GetTabletDocCount() const;
    std::shared_ptr<VersionDeployDescription> GetLoadedVersionDeployDescription() const;

    std::shared_ptr<TabletMetrics> GetTabletMetrics() const;

    void SetTabletId(const indexlib::framework::TabletId& tid);
    void SetMemoryStatus(const MemoryStatus& memoryStatus);
    void SetIndexRoot(const IndexRoot& indexRoot);
    void SetLoadedPublishVersion(const Version& version);
    void SetLoadedPrivateVersion(const Version& version);
    void SetLoadedVersionDeployDescription(const std::shared_ptr<VersionDeployDescription>& versionDpDesc);

    void SetBuildLocator(const Locator& locator);
    void SetLastCommittedLocator(const Locator& locator);
    void SetLastReopenLocator(const Locator& locator); // the new TabletData's locator after reopen
    void SetCounterMap(const std::shared_ptr<indexlib::util::CounterMap>& counterMap);
    Status InitCounter(bool isOnline);
    void UpdateTabletDocCount(int64_t tabletdocCount);
    void SetTabletMetrics(const std::shared_ptr<TabletMetrics>& tabletMetrics);

    static const char* MemoryStatusToStr(MemoryStatus status);

private:
    indexlib::framework::TabletId _tabletId;
    std::string _tabletName;
    MemoryStatus _memoryStatus;
    IndexRoot _indexRoot;
    Version _loadedPublishVersion;
    Version _loadedPrivateVersion;
    Locator _buildLocator;
    Locator _lastCommittedLocator;
    Locator _lastReopenLocator;
    std::shared_ptr<indexlib::util::CounterMap> _counterMap;
    std::shared_ptr<indexlib::util::StateCounter> _tabletDocCounter;
    std::shared_ptr<TabletMetrics> _tabletMetrics;
    std::shared_ptr<indexlibv2::framework::VersionDeployDescription> _loadedVersionDpDesc;

    mutable autil::ReadWriteLock _rwlock;
};

} // namespace indexlibv2::framework
