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
#include "indexlib/framework/TabletInfos.h"

#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

namespace indexlibv2::framework {

TabletInfos::TabletInfos()
    : _memoryStatus(MemoryStatus::OK)
    , _counterMap(std::make_shared<indexlib::util::CounterMap>())
{
}
TabletInfos::~TabletInfos() = default;

const indexlib::framework::TabletId& TabletInfos::GetTabletId() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _tabletId;
}
const std::string& TabletInfos::GetTabletName() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _tabletName;
}
const IndexRoot& TabletInfos::GetIndexRoot() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _indexRoot;
}
MemoryStatus TabletInfos::GetMemoryStatus() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _memoryStatus;
}
const std::shared_ptr<indexlib::util::CounterMap>& TabletInfos::GetCounterMap() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _counterMap;
}
Version TabletInfos::GetLoadedPublishVersion() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _loadedPublishVersion.Clone();
}
Version TabletInfos::GetLoadedPrivateVersion() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _loadedPrivateVersion.Clone();
}
std::shared_ptr<VersionDeployDescription> TabletInfos::GetLoadedVersionDeployDescription() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _loadedVersionDpDesc;
}
Locator TabletInfos::GetBuildLocator() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _buildLocator;
}
int64_t TabletInfos::GetTabletDocCount() const
{
    autil::ScopedReadLock lock(_rwlock);
    assert(_tabletDocCounter);
    return _tabletDocCounter->Get();
}

Locator TabletInfos::GetLastCommittedLocator() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _lastCommittedLocator;
}

Locator TabletInfos::GetLatestLocator() const
{
    autil::ScopedReadLock lock(_rwlock);
    auto locator = GetBuildLocator();
    if (locator.IsValid()) {
        return locator;
    }
    return _lastReopenLocator;
}

std::shared_ptr<TabletMetrics> TabletInfos::GetTabletMetrics() const
{
    autil::ScopedReadLock lock(_rwlock);
    return _tabletMetrics;
}

void TabletInfos::SetMemoryStatus(const MemoryStatus& memoryStatus)
{
    autil::ScopedWriteLock lock(_rwlock);
    _memoryStatus = memoryStatus;
}
void TabletInfos::SetIndexRoot(const IndexRoot& indexRoot)
{
    autil::ScopedWriteLock lock(_rwlock);
    _indexRoot = indexRoot;
}
void TabletInfos::SetLoadedPublishVersion(const Version& version)
{
    autil::ScopedWriteLock lock(_rwlock);
    _loadedPublishVersion = version.Clone();
}
void TabletInfos::SetLoadedVersionDeployDescription(const std::shared_ptr<VersionDeployDescription>& versionDpDesc)
{
    autil::ScopedWriteLock lock(_rwlock);
    _loadedVersionDpDesc = versionDpDesc;
}
void TabletInfos::SetLoadedPrivateVersion(const Version& version)
{
    autil::ScopedWriteLock lock(_rwlock);
    _loadedPrivateVersion = version.Clone();
}
void TabletInfos::SetBuildLocator(const Locator& locator)
{
    autil::ScopedWriteLock lock(_rwlock);
    _buildLocator = locator;
}
void TabletInfos::SetLastCommittedLocator(const Locator& lastCommittedLocator)
{
    autil::ScopedWriteLock lock(_rwlock);
    _lastCommittedLocator = lastCommittedLocator;
}
void TabletInfos::SetLastReopenLocator(const Locator& locator)
{
    autil::ScopedWriteLock lock(_rwlock);
    _lastReopenLocator = locator;
}
void TabletInfos::SetCounterMap(const std::shared_ptr<indexlib::util::CounterMap>& counterMap)
{
    autil::ScopedWriteLock lock(_rwlock);
    _counterMap = counterMap;
}
Status TabletInfos::InitCounter(bool isOnline)
{
    autil::ScopedWriteLock lock(_rwlock);
    std::string counterPath = isOnline ? "online.tabletDocCount" : "offline.tabletDocCount";
    auto tabletDocCounter = _counterMap->GetStateCounter(counterPath);
    if (!tabletDocCounter) {
        return Status::ConfigError("init counter failed", counterPath.c_str());
    }
    _tabletDocCounter = tabletDocCounter;
    return Status::OK();
}
void TabletInfos::UpdateTabletDocCount(int64_t tabletdocCount)
{
    autil::ScopedWriteLock lock(_rwlock);
    assert(_tabletDocCounter);
    _tabletDocCounter->Set(tabletdocCount);
}
void TabletInfos::SetTabletId(const indexlib::framework::TabletId& tabletId)
{
    autil::ScopedWriteLock lock(_rwlock);
    _tabletId = tabletId;
    _tabletName = tabletId.GenerateTabletName();
}
void TabletInfos::SetTabletMetrics(const std::shared_ptr<TabletMetrics>& tabletMetrics)
{
    autil::ScopedWriteLock lock(_rwlock);
    _tabletMetrics = tabletMetrics;
}

const char* TabletInfos::MemoryStatusToStr(MemoryStatus status)
{
    switch (status) {
    case MemoryStatus::OK:
        return "OK";
    case MemoryStatus::REACH_MAX_RT_INDEX_SIZE:
        return "REACH_MAX_RT_INDEX_SIZE";
    case MemoryStatus::REACH_TOTAL_MEM_LIMIT:
        return "REACH_TOTAL_MEM_LIMIT";
    case MemoryStatus::REACH_LOW_WATERMARK_MEM_LIMIT:
        return "REACH_LOW_WATERMARK_MEM_LIMIT";
    case MemoryStatus::REACH_HIGH_WATERMARK_MEM_LIMIT:
        return "REACH_HIGH_WATERMARK_MEM_LIMIT";
    case MemoryStatus::UNKNOWN:
        return "UNKNOWN";
    default:
        assert(false);
        return "UNEXPECTED";
    }
}

} // namespace indexlibv2::framework
