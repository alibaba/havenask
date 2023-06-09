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

#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentMeta.h"

namespace indexlib::framework {
class SegmentMetrics;
} // namespace indexlib::framework

namespace indexlibv2::index {
class IIndexer;
}

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {

class Segment : private autil::NoCopyable
{
public:
    // ST_BUILDING: a MemSegment, current building
    // ST_DUMPING: a MemSegment, in dumping Queue, convert from ST_BUILDING
    // ST_BUILT: a DiskSegment, load from disk
    // ST_UNSPECIFY: ALL
    enum class SegmentStatus { ST_UNSPECIFY, ST_BUILT, ST_DUMPING, ST_BUILDING };

    Segment(SegmentStatus segmentStatus) : _segmentStatus(segmentStatus) {}
    virtual ~Segment() = default;

    // Get indexer for the given type and indexName.
    // Mem indexers always load the data.
    // For disk indexers, data will be loaded in non-lazy mode.
    virtual std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                                       const std::string& indexName)
    {
        return std::make_pair(Status::NotFound(), nullptr);
    }

    virtual void AddIndexer(const std::string& type, const std::string& indexName,
                            std::shared_ptr<indexlibv2::index::IIndexer> indexer)
    {
        assert(false);
    }
    virtual void DeleteIndexer(const std::string& type, const std::string& indexName) { assert(false); }

    virtual size_t EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema) = 0;
    virtual size_t EvaluateCurrentMemUsed() = 0;
    virtual void CollectSegmentDescription(const std::shared_ptr<SegmentDescriptions>& segDescs) {}
    segmentid_t GetSegmentId() const { return _segmentMeta.segmentId; }
    const std::shared_ptr<SegmentInfo>& GetSegmentInfo() const { return _segmentMeta.segmentInfo; }
    const std::shared_ptr<indexlib::file_system::Directory>& GetSegmentDirectory() const
    {
        return _segmentMeta.segmentDir;
    }
    std::shared_ptr<indexlib::framework::SegmentMetrics> GetSegmentMetrics() const
    {
        return _segmentMeta.segmentMetrics;
    }
    std::shared_ptr<config::TabletSchema> GetSegmentSchema() const { return _segmentMeta.schema; }
    SegmentStatus GetSegmentStatus() const { return _segmentStatus; }
    void SetSegmentStatus(const SegmentStatus& segmentStatus) { _segmentStatus = segmentStatus; }
    static bool IsRtSegmentId(segmentid_t segId) { return (segId & RT_SEGMENT_ID_MASK) > 0; }
    static bool IsMergedSegmentId(segmentid_t segId)
    {
        return segId != INVALID_SEGMENTID && (segId & (PUBLIC_SEGMENT_ID_MASK | PRIVATE_SEGMENT_ID_MASK)) == 0;
    }

    static bool IsPublicSegmentId(segmentid_t segId)
    {
        return segId != INVALID_SEGMENTID && (segId & PUBLIC_SEGMENT_ID_MASK);
    }
    static bool IsPrivateSegmentId(segmentid_t segId)
    {
        return segId != INVALID_SEGMENTID && (segId & PRIVATE_SEGMENT_ID_MASK);
    }

    std::pair<Status, Locator> GetLastLocator()
    {
        Locator locator = GetSegmentInfo()->GetLocator();
        return {Status::OK(), locator};
    }

    static constexpr segmentid_t RT_SEGMENT_ID_MASK = (segmentid_t)0x1 << 30;
    static constexpr segmentid_t MERGED_SEGMENT_ID_MASK = (segmentid_t)0x0;

    static constexpr segmentid_t PUBLIC_SEGMENT_ID_MASK = (segmentid_t)0x1 << 29;
    static constexpr segmentid_t PRIVATE_SEGMENT_ID_MASK = (segmentid_t)0x1 << 30;

    void TEST_SetSegmentMeta(const SegmentMeta& segmentMeta) { _segmentMeta = segmentMeta; }

protected:
    void SetSegmentMeta(const SegmentMeta& segmentMeta) { _segmentMeta = segmentMeta; }

    SegmentStatus _segmentStatus = SegmentStatus::ST_UNSPECIFY;
    SegmentMeta _segmentMeta;
};

typedef std::shared_ptr<Segment> SegmentPtr;

} // namespace indexlibv2::framework
