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

#include <any>
#include <memory>
#include <mutex>
#include <vector>

#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {
class ResourceMap;
class TabletDataInfo;
struct TabletDataSchemaGroup;
class TabletData : private autil::NoCopyable
{
public:
    using SegmentPtr = std::shared_ptr<Segment>;

    class Slice
    {
    public:
        using const_iterator = std::vector<SegmentPtr>::const_iterator;
        using const_reverse_iterator = std::vector<SegmentPtr>::const_reverse_iterator;

    public:
        Slice(const_iterator beginIter, const_iterator endIter, const_reverse_iterator rbeginIter,
              const_reverse_iterator rendIter)
            : _cBegin(beginIter)
            , _cEnd(endIter)
            , _cRbegin(rbeginIter)
            , _cRend(rendIter)
        {
        }
        auto begin() { return _cBegin; }
        auto end() { return _cEnd; }
        auto rbegin() { return _cRbegin; }
        auto rend() { return _cRend; }
        auto cbegin() const { return _cBegin; }
        auto cend() const { return _cEnd; }
        auto crbegin() const { return _cRbegin; }
        auto crend() const { return _cRend; }

        bool empty() const { return _cBegin == _cEnd; }

    private:
        const_iterator _cBegin;
        const_iterator _cEnd;
        const_reverse_iterator _cRbegin;
        const_reverse_iterator _cRend;
    };

    explicit TabletData(std::string tabletName);
    ~TabletData();

    std::string GetTabletName() const { return _tabletName; }
    const Version& GetOnDiskVersion() const;
    Slice CreateSlice(Segment::SegmentStatus segmentStatus) const;
    // return all segments
    Slice CreateSlice() const { return CreateSlice(Segment::SegmentStatus::ST_UNSPECIFY); };
    SegmentPtr GetSegment(segmentid_t segmentId) const;

    Status Init(Version onDiskVersion, std::vector<SegmentPtr> segments,
                const std::shared_ptr<ResourceMap>& resourceMap);
    uint64_t GetTabletDocCount() const;
    size_t GetIncSegmentCount() const;
    size_t GetSegmentCount() const { return _segments.size(); }

    void DeclareTaskConfig(const std::string& taskName, const std::string& taskType);

    Locator GetLocator() const;
    const std::shared_ptr<ResourceMap>& GetResourceMap() const { return _resourceMap; }
    void ReclaimSegmentResource();
    std::shared_ptr<const TabletDataInfo> GetTabletDataInfo() const { return _tabletDataInfo; }
    void TEST_PushSegment(SegmentPtr segment)
    {
        _segments.push_back(segment);
        if (_hasBuildingSegment) {
            ++_dumpingSegmentCount;
        } else {
            _hasBuildingSegment = true;
        }
    }
    void TEST_SetOnDiskVersion(Version version) { _onDiskVersion = version; }
    const std::vector<std::shared_ptr<Segment>>& TEST_GetSegments() { return _segments; }
    void RefreshDocCount();
    std::shared_ptr<config::TabletSchema> GetOnDiskVersionReadSchema() const;
    std::shared_ptr<config::TabletSchema> GetOnDiskVersionWriteSchema() const;
    std::shared_ptr<config::TabletSchema> GetWriteSchema() const;
    std::shared_ptr<config::TabletSchema> GetTabletSchema(schemaid_t schemaId) const;
    std::vector<std::shared_ptr<config::TabletSchema>> GetAllTabletSchema(schemaid_t beginSchemaId,
                                                                          schemaid_t endSchemaId) const;

private:
    TabletDataSchemaGroup* GetTabletDataSchemaGroup() const;
    bool CheckSchemaIdLegal(schemaid_t schemaId) const;

private:
    std::string _tabletName;
    Version _onDiskVersion;

    std::vector<std::shared_ptr<Segment>> _segments;
    std::shared_ptr<ResourceMap> _resourceMap;
    size_t _builtSegmentCount;
    size_t _dumpingSegmentCount;
    std::shared_ptr<TabletDataInfo> _tabletDataInfo;
    bool _hasBuildingSegment;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
