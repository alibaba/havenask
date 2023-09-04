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
#include "indexlib/framework/TabletReaderContainer.h"

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/framework/VersionDeployDescription.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, TabletReaderContainer);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)
void TabletReaderContainer::AddTabletReader(const std::shared_ptr<TabletData>& tabletData,
                                            const ITabletReaderPtr& tabletReader,
                                            const std::shared_ptr<VersionDeployDescription>& versionDeployDescription)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _tabletReaderVec.emplace_back(tabletData, tabletReader, versionDeployDescription);
    auto versionId = tabletData->GetOnDiskVersion().GetVersionId();
    if (framework::Version::PRIVATE_VERSION_ID_MASK & versionId) {
        _latestPrivateVersion = std::max(_latestPrivateVersion, versionId);
    }
    TABLET_LOG(INFO, "add reader succcess, ondisk version id[%d]", versionId);
}

bool TabletReaderContainer::HasTabletReader(versionid_t versionId) const
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, __] : _tabletReaderVec) {
        if (tabletData->GetOnDiskVersion().GetVersionId() == versionId) {
            return true;
        }
    }
    return false;
}

bool TabletReaderContainer::IsUsingSegment(segmentid_t segmentId) const
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, __] : _tabletReaderVec) {
        auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_UNSPECIFY);
        for (auto it = slice.begin(); it != slice.end(); ++it) {
            if ((*it)->GetSegmentId() == segmentId) {
                return true;
            }
        }
    }
    return false;
}

void TabletReaderContainer::GetIncVersions(std::vector<Version>& versions) const
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, __] : _tabletReaderVec) {
        versions.emplace_back(tabletData->GetOnDiskVersion().Clone());
    }
}

versionid_t TabletReaderContainer::GetLatestReaderVersionId() const
{
    std::lock_guard<std::mutex> guard(_mutex);

    if (!_tabletReaderVec.empty()) {
        auto [tabletData, _, __] = _tabletReaderVec.back();
        return tabletData->GetOnDiskVersion().GetVersionId();
    }
    return INVALID_VERSIONID;
}

std::shared_ptr<ITabletReader> TabletReaderContainer::GetOldestTabletReader() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (!_tabletReaderVec.empty()) {
        auto [_, reader, __] = *(_tabletReaderVec.begin());
        return reader;
    }
    return nullptr;
}

std::shared_ptr<ITabletReader> TabletReaderContainer::GetLatestTabletReader() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (!_tabletReaderVec.empty()) {
        auto [_, reader, __] = *(_tabletReaderVec.rbegin());
        return reader;
    }
    return nullptr;
}

size_t TabletReaderContainer::Size() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return _tabletReaderVec.size();
}

void TabletReaderContainer::Close()
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (size_t i = 0; i < _tabletReaderVec.size(); i++) {
        auto& [tabletData, reader, __] = _tabletReaderVec[i];
        size_t useCount = reader.use_count();
        if (useCount > 1) {
            TABLET_LOG(ERROR,
                       "[%s] unreleased reader [version: %d] [use_count: %ld] exists, when TabletReaderContainer is to "
                       "be closed",
                       _tabletName.c_str(), tabletData->GetOnDiskVersion().GetVersionId(), useCount);
        }
    }
    return _tabletReaderVec.clear();
}

void TabletReaderContainer::EvictOldestTabletReader()
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (!_tabletReaderVec.empty()) {
        auto [oldestTabletData, reader, __] = *(_tabletReaderVec.begin());
        TABLET_LOG(INFO, "remove reader succcess, ondisk version id[%d]",
                   oldestTabletData->GetOnDiskVersion().GetVersionId());
        _tabletReaderVec.erase(_tabletReaderVec.begin());
    }
    return;
}

std::shared_ptr<ITabletReader> TabletReaderContainer::TEST_GetTabletReader(versionid_t versionId) const
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, tabletReader, _] : _tabletReaderVec) {
        if (tabletData->GetOnDiskVersion().GetVersionId() == versionId) {
            return tabletReader;
        }
    }
    return nullptr;
}

versionid_t TabletReaderContainer::GetOldestIncVersionId() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletReaderVec.empty()) {
        return INVALID_VERSIONID;
    }
    auto [tabletData, _, __] = *(_tabletReaderVec.begin());
    assert(tabletData != nullptr);
    return tabletData->GetOnDiskVersion().GetVersionId();
}

versionid_t TabletReaderContainer::GetLatestIncVersionId() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletReaderVec.empty()) {
        return INVALID_VERSIONID;
    }
    auto tabletData = std::get<0>(_tabletReaderVec.back());
    assert(tabletData != nullptr);
    return tabletData->GetOnDiskVersion().GetVersionId();
}

int64_t TabletReaderContainer::GetLatestIncVersionTimestamp() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletReaderVec.empty()) {
        return INVALID_TIMESTAMP;
    }
    auto tabletData = std::get<0>(_tabletReaderVec.back());
    assert(tabletData != nullptr);
    return tabletData->GetOnDiskVersion().GetTimestamp();
}

int64_t TabletReaderContainer::GetLatestIncVersionTaskLogTimestamp() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (_tabletReaderVec.empty()) {
        return INVALID_TIMESTAMP;
    }
    auto tabletData = std::get<0>(_tabletReaderVec.back());
    assert(tabletData != nullptr);
    return tabletData->GetOnDiskVersion().GetIndexTaskHistory().GetLatestTaskLogTimestamp();
}

std::vector<std::shared_ptr<TabletData>> TabletReaderContainer::GetTabletDatas() const
{
    std::vector<std::shared_ptr<TabletData>> tabletDatas;
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, __] : _tabletReaderVec) {
        tabletDatas.emplace_back(tabletData);
    }
    return tabletDatas;
}

bool TabletReaderContainer::GetNeedKeepFiles(std::set<std::string>* keepFiles)
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, versionDeployDescription] : _tabletReaderVec) {
        if (!versionDeployDescription ||
            !versionDeployDescription->SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST)) {
            TABLET_LOG(INFO, "versionDpDesc is null or disable meta manifest, don't need clean, versionId[%d]",
                       tabletData->GetOnDiskVersion().GetVersionId());
            return false;
        }
        for (const auto& deployMeta : versionDeployDescription->localDeployIndexMetas) {
            for (const auto& fileMeta : deployMeta->deployFileMetas) {
                keepFiles->insert(fileMeta.filePath);
            }
        }
    }
    return true;
}

void TabletReaderContainer::GetNeedKeepSegments(std::set<segmentid_t>* keepSegments)
{
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& [tabletData, _, versionDeployDescription] : _tabletReaderVec) {
        const auto& version = tabletData->GetOnDiskVersion();
        for (auto segment : version) {
            keepSegments->insert(segment.segmentId);
        }
    }
}

versionid_t TabletReaderContainer::GetLatestPrivateVersion() const { return _latestPrivateVersion; }

} // namespace indexlibv2::framework
