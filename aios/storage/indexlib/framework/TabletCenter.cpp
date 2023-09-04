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
#include "indexlib/framework/TabletCenter.h"

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/TabletInfos.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletCenter);

TabletCenter::TabletCenter() : _enable(false) {}

TabletCenter::~TabletCenter()
{
    _enable = false;
    _tabletHandles.clear();
}

void TabletCenter::Activate()
{
    if (autil::EnvUtil::getEnv("INDEXLIB_DISABLE_TABLET_CENTER", false)) {
        AUTIL_LOG(INFO, "disable TabletCenter by ENV.");
        return;
    }

    _enable = true;
    if (!_evictLoopThread) {
        _evictLoopThread =
            autil::LoopThread::createLoopThread(std::bind(&TabletCenter::EvictClosedTables, this), 10 * 1000, /* 10ms */
                                                "TabletCenterEvict");
    }
    AUTIL_LOG(INFO, "enable TabletCenter.");
}

void TabletCenter::Deactivate()
{
    _enable = false;
    AUTIL_LOG(INFO, "deactivate TabletCenter.");
}

std::shared_ptr<TabletCenter::TabletResourceHandler>
TabletCenter::RegisterTablet(const std::shared_ptr<ITablet>& tablet)
{
    if (!tablet) {
        return std::shared_ptr<TabletResourceHandler>();
    }

    auto tabletName = tablet->GetTabletInfos()->GetTabletName();
    if (!_enable) {
        AUTIL_LOG(INFO, "tablet [%s] will not be registered, because INDEXLIBV2_ENABLE_TABLET_CENTER=false.",
                  tabletName.c_str());
        return std::shared_ptr<TabletResourceHandler>();
    }

    auto handle = std::make_shared<TabletResourceHandler>(tablet);
    AUTIL_LOG(INFO, "Register handle for tablet [%s], object [%p].", tabletName.c_str(), tablet.get());
    auto tabletSchema = tablet->GetTabletSchema();
    std::lock_guard<std::mutex> guard(_mutex);
    if (FindTabletResourceHandleUnsafe(tablet.get()) != nullptr) {
        AUTIL_LOG(ERROR, "tablet object for [%s] should only be registered once.", tabletName.c_str());
        return std::shared_ptr<TabletResourceHandler>();
    }

    uint32_t idx = (uint32_t)_tabletHandles.size();
    _tabletHandles.emplace_back(handle);
    _tabletName2IdxMap[tabletName].push_back(idx);
    if (tabletSchema) {
        _schemaName2IdxMap[tabletSchema->GetTableName()].push_back(idx);
    }
    return handle;
}

void TabletCenter::UnregisterTablet(ITablet* tablet)
{
    if (!_enable) {
        return;
    }

    assert(tablet);
    std::shared_ptr<ITablet> toReleaseTablet; // make tablet destruction not in lock scope
    {
        std::lock_guard<std::mutex> guard(_mutex);
        auto handle = FindTabletResourceHandleUnsafe(tablet);
        if (handle) {
            AUTIL_LOG(INFO, "Unregister Tablet [%s], object [%p]", tablet->GetTabletInfos()->GetTabletName().c_str(),
                      tablet);
            toReleaseTablet = handle->GetTabletPtr();
            handle->Reset();
        }
    }
}

bool TabletCenter::UpdateDocumentParser(const std::string& tabletName,
                                        const std::shared_ptr<document::IDocumentParser>& parser)
{
    std::lock_guard<std::mutex> guard(_mutex);
    auto iter = _tabletName2IdxMap.find(tabletName);
    if (iter == _tabletName2IdxMap.end()) {
        AUTIL_LOG(INFO, "update document parser fail, not tablet [%s] exist", tabletName.c_str());
        return false;
    }

    for (auto idx : iter->second) {
        assert(idx < _tabletHandles.size());
        auto tablet = _tabletHandles[idx]->GetTabletPtr();
        if (tablet) {
            AUTIL_LOG(INFO, "update document parser for tablet [%s].", tabletName.c_str());
            _tabletHandles[idx]->SetDocumentParser(parser);
        }
    }
    return true;
}

void TabletCenter::GetTabletByTabletName(const std::string& tabletName, std::vector<TabletResource>& handlers) const
{
    handlers.clear();
    std::lock_guard<std::mutex> guard(_mutex);
    auto iter = _tabletName2IdxMap.find(tabletName);
    if (iter == _tabletName2IdxMap.end()) {
        return;
    }

    handlers.reserve(iter->second.size());
    for (auto idx : iter->second) {
        assert(idx < _tabletHandles.size());
        auto tablet = _tabletHandles[idx]->GetTabletPtr();
        if (tablet) {
            TabletResource tmp;
            tmp.tabletPtr = tablet;
            tmp.docParserPtr = _tabletHandles[idx]->GetDocumentParser();
            handlers.emplace_back(tmp);
        }
    }
}

void TabletCenter::GetTabletBySchemaName(const std::string& schemaName, std::vector<TabletResource>& handlers) const
{
    handlers.clear();
    std::lock_guard<std::mutex> guard(_mutex);
    auto iter = _schemaName2IdxMap.find(schemaName);
    if (iter == _schemaName2IdxMap.end()) {
        return;
    }

    handlers.reserve(iter->second.size());
    for (auto idx : iter->second) {
        assert(idx < _tabletHandles.size());
        auto tablet = _tabletHandles[idx]->GetTabletPtr();
        if (tablet) {
            TabletResource tmp;
            tmp.tabletPtr = tablet;
            tmp.docParserPtr = _tabletHandles[idx]->GetDocumentParser();
            handlers.emplace_back(tmp);
        }
    }
}

void TabletCenter::GetAllTablets(std::vector<TabletResource>& handlers) const
{
    handlers.clear();
    std::lock_guard<std::mutex> guard(_mutex);
    handlers.reserve(_tabletHandles.size());
    for (auto& handle : _tabletHandles) {
        auto tablet = handle->GetTabletPtr();
        if (tablet) {
            TabletResource tmp;
            tmp.tabletPtr = tablet;
            tmp.docParserPtr = handle->GetDocumentParser();
            handlers.emplace_back(tmp);
        }
    }
}

void TabletCenter::EvictClosedTables()
{
    int64_t currentTs = autil::TimeUtility::currentTime();
    if (currentTs - _lastTs < EVICT_LOOP_INTERVAL) {
        return;
    }

    _lastTs = currentTs;
    TabletHandleVec totalHandles;
    {
        std::lock_guard<std::mutex> guard(_mutex);
        totalHandles = _tabletHandles;
    }

    TabletHandleVec validHandles;
    Name2IndexMap newTabletIdxMap;
    Name2IndexMap newSchemaIdxMap;
    validHandles.reserve(totalHandles.size());
    for (auto& handle : totalHandles) {
        auto tablet = handle->GetTabletPtr();
        if (!tablet) {
            AUTIL_LOG(INFO, "Evict one invalid tablet handle in TabletCenter.");
            continue;
        }

        if (tablet.use_count() == 2) { // tablet & totalHandles
            AUTIL_LOG(INFO, "Evict orphan tablet handle, which only be held by tablet center.");
            continue;
        }

        uint32_t idx = (uint32_t)validHandles.size();
        validHandles.emplace_back(handle);
        auto tabletName = tablet->GetTabletInfos()->GetTabletName();
        newTabletIdxMap[tabletName].push_back(idx);
        auto tabletSchema = tablet->GetTabletSchema();
        if (tabletSchema) {
            newSchemaIdxMap[tabletSchema->GetTableName()].push_back(idx);
        }
    }

    if (validHandles.size() == totalHandles.size()) {
        // all handles is valid, do nothing
        return;
    }

    std::lock_guard<std::mutex> guard(_mutex);
    for (size_t i = totalHandles.size(); i < _tabletHandles.size(); i++) {
        // new tablet added when _mutex is not locked
        auto& handle = _tabletHandles[i];
        auto tablet = handle->GetTabletPtr();
        if (!tablet) {
            AUTIL_LOG(INFO, "Evict one invalid tablet handle in TabletCenter.");
            continue;
        }

        if (tablet.use_count() == 2) {
            AUTIL_LOG(INFO, "Evict orphan tablet handle, which only be held by tablet center.");
            continue;
        }
        uint32_t idx = (uint32_t)validHandles.size();
        validHandles.emplace_back(handle);
        auto tabletName = tablet->GetTabletInfos()->GetTabletName();
        newTabletIdxMap[tabletName].push_back(idx);
        auto tabletSchema = tablet->GetTabletSchema();
        if (tabletSchema) {
            newSchemaIdxMap[tabletSchema->GetTableName()].push_back(idx);
        }
    }

    AUTIL_LOG(INFO, "[%lu] valid tablet handles left in TabletCenter.", validHandles.size());
    _tabletHandles.swap(validHandles);
    _tabletName2IdxMap.swap(newTabletIdxMap);
    _schemaName2IdxMap.swap(newSchemaIdxMap);
}

std::shared_ptr<TabletCenter::TabletResourceHandler> TabletCenter::FindTabletResourceHandleUnsafe(ITablet* tablet) const
{
    for (auto& handle : _tabletHandles) {
        auto tabletPtr = handle->GetTabletPtr();
        if (tabletPtr.get() == tablet) {
            return handle;
        }
    }
    return std::shared_ptr<TabletResourceHandler>();
}

} // namespace indexlibv2::framework
