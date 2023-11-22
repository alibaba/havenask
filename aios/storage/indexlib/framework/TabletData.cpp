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
#include "indexlib/framework/TabletData.h"

#include <algorithm>
#include <assert.h>
#include <map>
#include <set>
#include <type_traits>

#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletDataInfo.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/index_task/IndexTaskHistory.h"

#define TABLET_NAME _tabletName.c_str()

namespace indexlibv2::framework {
using SegmentPtr = std::shared_ptr<Segment>;
AUTIL_LOG_SETUP(indexlib.framework, TabletData);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

TabletData::TabletData(std::string tabletName)
    : _tabletName(std::move(tabletName))
    , _builtSegmentCount(0)
    , _dumpingSegmentCount(0)
    , _tabletDataInfo(new TabletDataInfo)
    , _hasBuildingSegment(false)
{
}

TabletData::~TabletData()
{
    if (!autil::StringUtil::startsWith(_tabletName, "__TMP__")) {
        TABLET_LOG(INFO, "release tablet data, version[%d]", _onDiskVersion.GetVersionId());
    }
}

Status TabletData::Init(Version onDiskVersion, std::vector<SegmentPtr> segments,
                        const std::shared_ptr<ResourceMap>& resourceMap)
{
    assert(resourceMap);
    static_assert(Segment::SegmentStatus::ST_UNSPECIFY < Segment::SegmentStatus::ST_BUILT);
    static_assert(Segment::SegmentStatus::ST_BUILT < Segment::SegmentStatus::ST_DUMPING);
    static_assert(Segment::SegmentStatus::ST_DUMPING < Segment::SegmentStatus::ST_BUILDING);

    std::vector<segmentid_t> segIds;
    for (auto segment : segments) {
        segIds.push_back(segment->GetSegmentId());
    }
    TABLET_LOG(INFO, "init tablet data, version[%d], [%lu] segments [%s]", onDiskVersion.GetVersionId(), segIds.size(),
               autil::legacy::ToJsonString(segIds, true).c_str());

    auto lastSegmentStatus = Segment::SegmentStatus::ST_UNSPECIFY;
    _builtSegmentCount = 0;
    _dumpingSegmentCount = 0;
    _hasBuildingSegment = false;
    for (const auto& seg : segments) {
        auto segmentStatus = seg->GetSegmentStatus();
        if (segmentStatus < lastSegmentStatus) {
            TABLET_LOG(ERROR, "invalid segment sequence %d", seg->GetSegmentId());
            return Status::Corruption("invalid segment sequence %d", seg->GetSegmentId());
        }
        lastSegmentStatus = segmentStatus;
        switch (segmentStatus) {
        case Segment::SegmentStatus::ST_BUILT:
            ++_builtSegmentCount;
            break;
        case Segment::SegmentStatus::ST_DUMPING:
            ++_dumpingSegmentCount;
            break;
        case Segment::SegmentStatus::ST_BUILDING:
            if (_hasBuildingSegment) {
                TABLET_LOG(ERROR, "duplicate building segment [%d]", seg->GetSegmentId());
                return Status::Corruption("duplicate building segment [%d]", seg->GetSegmentId());
            }
            _hasBuildingSegment = true;
            break;
        default:
            TABLET_LOG(ERROR, "unspecified type segment [%d]", seg->GetSegmentId());
            return Status::Corruption("unspecified type segment [%d]", seg->GetSegmentId());
        }
    }
    _segments = std::move(segments);
    _onDiskVersion = onDiskVersion.Clone();
    _resourceMap = resourceMap;
    RefreshDocCount();
    return Status::OK();
}

TabletData::Slice TabletData::CreateSlice(Segment::SegmentStatus status) const
{
    if (status == Segment::SegmentStatus::ST_BUILT) {
        return TabletData::Slice(_segments.begin(), _segments.begin() + _builtSegmentCount,
                                 _segments.rend() - _builtSegmentCount, _segments.rend());
    } else if (status == Segment::SegmentStatus::ST_DUMPING) {
        return TabletData::Slice(
            _segments.begin() + _builtSegmentCount, _segments.begin() + _builtSegmentCount + _dumpingSegmentCount,
            _segments.rend() - _builtSegmentCount - _dumpingSegmentCount, _segments.rend() - _builtSegmentCount);
    } else if (status == Segment::SegmentStatus::ST_BUILDING) {
        if (!_hasBuildingSegment) {
            return TabletData::Slice(_segments.end(), _segments.end(), _segments.rend(), _segments.rend());
        }
        return TabletData::Slice(_segments.end() - 1, _segments.end(), _segments.rbegin(), _segments.rbegin() + 1);
    } else if (status == Segment::SegmentStatus::ST_UNSPECIFY) {
        return TabletData::Slice(_segments.begin(), _segments.end(), _segments.rbegin(), _segments.rend());
    } else {
        assert(false);
    }
    return TabletData::Slice(_segments.end(), _segments.end(), _segments.rend(), _segments.rend());
}

const Version& TabletData::GetOnDiskVersion() const { return _onDiskVersion; }

Locator TabletData::GetLocator() const
{
    std::shared_ptr<Segment> lastSegment;
    if (!_segments.empty()) {
        lastSegment = *_segments.rbegin();
    }
    if (lastSegment && !_onDiskVersion.HasSegment(lastSegment->GetSegmentId())) {
        return lastSegment->GetSegmentInfo()->GetLocator();
    }
    return _onDiskVersion.GetLocator();
}

TabletData::SegmentPtr TabletData::GetSegment(segmentid_t segmentId) const
{
    auto slice = CreateSlice();
    for (auto seg : slice) {
        if (seg->GetSegmentId() == segmentId) {
            return seg;
        }
    }
    return nullptr;
}

std::pair<SegmentPtr, docid64_t> TabletData::GetSegmentWithBaseDocid(segmentid_t segmentId)
{
    docid64_t baseDocId = 0;
    auto slice = CreateSlice(framework::Segment::SegmentStatus::ST_UNSPECIFY);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        if (segmentId == (*iter)->GetSegmentId()) {
            return std::make_pair(*iter, baseDocId);
        }
        baseDocId += (*iter)->GetSegmentInfo()->docCount;
    }
    return std::make_pair(nullptr, INVALID_DOCID);
}

uint64_t TabletData::GetTabletDocCount() const { return _tabletDataInfo->GetDocCount(); }

size_t TabletData::GetIncSegmentCount() const
{
    auto builtSlice = CreateSlice(Segment::SegmentStatus::ST_BUILT);
    uint64_t incBuiltSegmentCount = 0;
    for (auto& seg : builtSlice) {
        if (_onDiskVersion.HasSegment(seg->GetSegmentId())) {
            ++incBuiltSegmentCount;
        }
    }
    return incBuiltSegmentCount;
}

void TabletData::ReclaimSegmentResource()
{
    std::set<segmentid_t> segmentIds;
    for (const auto& segment : _segments) {
        segmentIds.insert(segment->GetSegmentId());
    }
    _resourceMap->ReclaimSegmentResource(segmentIds);
}

void TabletData::RefreshDocCount()
{
    uint64_t totalDocCount = 0;
    auto allSlice = CreateSlice();
    for (auto& seg : allSlice) {
        totalDocCount += seg->GetSegmentInfo()->GetDocCount();
    }
    _tabletDataInfo->SetDocCount(totalDocCount);
}

TabletDataSchemaGroup* TabletData::GetTabletDataSchemaGroup() const
{
    if (!_resourceMap) {
        return nullptr;
    }
    auto resource = _resourceMap->GetResource(TabletDataSchemaGroup::NAME);
    if (!resource) {
        return nullptr;
    }
    auto schemaGroup = dynamic_cast<TabletDataSchemaGroup*>(resource.get());
    if (!schemaGroup) {
        TABLET_LOG(ERROR, "[%s] type mismatched", TabletDataSchemaGroup::NAME);
        return nullptr;
    }
    return schemaGroup;
}

std::shared_ptr<config::ITabletSchema> TabletData::GetOnDiskVersionReadSchema() const
{
    auto schemaGroup = GetTabletDataSchemaGroup();
    if (!schemaGroup) {
        return nullptr;
    }
    return schemaGroup->onDiskReadSchema;
}

std::shared_ptr<config::ITabletSchema> TabletData::GetOnDiskVersionWriteSchema() const
{
    auto schemaGroup = GetTabletDataSchemaGroup();
    if (!schemaGroup) {
        return nullptr;
    }
    return schemaGroup->onDiskWriteSchema;
}

std::shared_ptr<config::ITabletSchema> TabletData::GetWriteSchema() const
{
    auto schemaGroup = GetTabletDataSchemaGroup();
    if (!schemaGroup) {
        return nullptr;
    }
    return schemaGroup->writeSchema;
}

bool TabletData::CheckSchemaIdLegal(schemaid_t schemaId) const
{
    auto schemaGroup = GetTabletDataSchemaGroup();
    if (!schemaGroup) {
        return false;
    }

    if (_onDiskVersion.GetSchemaId() == schemaId || _onDiskVersion.IsInSchemaVersionRoadMap(schemaId) ||
        schemaGroup->writeSchema->GetSchemaId() == schemaId) {
        return true;
    }

    for (const auto& segment : _segments) {
        if (segment->GetSegmentSchema()->GetSchemaId() == schemaId) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<config::ITabletSchema> TabletData::GetTabletSchema(schemaid_t schemaId) const
{
    if (!CheckSchemaIdLegal(schemaId)) {
        return nullptr;
    }
    auto schemaGroup = GetTabletDataSchemaGroup();
    auto iter = schemaGroup->multiVersionSchemas.find(schemaId);
    if (iter != schemaGroup->multiVersionSchemas.end()) {
        return iter->second;
    }
    return nullptr;
}

std::vector<std::shared_ptr<config::ITabletSchema>> TabletData::GetAllTabletSchema(schemaid_t beginSchemaId,
                                                                                   schemaid_t endSchemaId) const
{
    assert(endSchemaId >= beginSchemaId);
    std::vector<std::shared_ptr<config::ITabletSchema>> result;
    auto schemaGroup = GetTabletDataSchemaGroup();
    if (!schemaGroup) {
        return result;
    }
    auto iter = schemaGroup->multiVersionSchemas.lower_bound(beginSchemaId);
    for (; iter != schemaGroup->multiVersionSchemas.end(); iter++) {
        if (iter->first > endSchemaId) {
            break;
        }
        if (!_onDiskVersion.IsInSchemaVersionRoadMap(iter->first) &&
            schemaGroup->writeSchema->GetSchemaId() != iter->first) {
            TABLET_LOG(WARN, "schema [%d] not in schemaVersion read map [%s]", iter->first,
                       autil::StringUtil::toString(_onDiskVersion.GetSchemaVersionRoadMap(), ",").c_str());
            continue;
        }
        result.push_back(iter->second);
    }
    return result;
}

void TabletData::DeclareTaskConfig(const std::string& taskName, const std::string& taskType)
{
    auto& taskHistory = _onDiskVersion.GetIndexTaskHistory();
    if (taskHistory.DeclareTaskConfig(taskName, taskType)) {
        TABLET_LOG(INFO, "declare new task config, taskName [%s], taskType [%s]", taskName.c_str(), taskType.c_str());
    }
}

void TabletData::TEST_PushSegment(SegmentPtr segment)
{
    _segments.push_back(segment);
    if (_hasBuildingSegment) {
        ++_dumpingSegmentCount;
    } else {
        _hasBuildingSegment = true;
    }
}
void TabletData::TEST_SetOnDiskVersion(Version version) { _onDiskVersion = version; }
const std::vector<std::shared_ptr<Segment>>& TabletData::TEST_GetSegments() { return _segments; }

#undef TABLET_LOG

} // namespace indexlibv2::framework
