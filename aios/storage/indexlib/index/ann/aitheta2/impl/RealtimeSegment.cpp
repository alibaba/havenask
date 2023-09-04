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

#include "indexlib/index/ann/aitheta2/impl/RealtimeSegment.h"

#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaDumper.h"
using namespace indexlib::file_system;
using namespace autil;
using namespace std;
using namespace aitheta2;
namespace indexlibv2::index::ann {

RealtimeSegment::RealtimeSegment(const AithetaIndexConfig& indexConfig)
    : Segment(indexConfig, SegmentType::ST_REALTIME, true)
{
}
RealtimeSegment::~RealtimeSegment() { Close(); }

bool RealtimeSegment::Open(const SegmentBuildResourcePtr& segmentBuildResource)
{
    return InitStreamerMap(segmentBuildResource);
}

bool RealtimeSegment::InitStreamerMap(const SegmentBuildResourcePtr& segmentBuildResource)
{
    if (segmentBuildResource == nullptr) {
        return true;
    }

    auto realtimeSegmentBuildRes = dynamic_pointer_cast<RealtimeSegmentBuildResource>(segmentBuildResource);
    ANN_CHECK(realtimeSegmentBuildRes != nullptr, "dynamic cast failed, init streamer map failed");
    for (auto indexResource : realtimeSegmentBuildRes->GetAllRealtimeIndexBuildResources()) {
        AiThetaStreamerPtr streamer;
        ANN_CHECK(AiThetaFactoryWrapper::CreateStreamer(_indexConfig, indexResource, streamer),
                  "create streamer failed");
        _streamerMap[indexResource->indexId] = streamer;
    }
    return true;
}

bool RealtimeSegment::GetRealtimeIndex(index_id_t indexId, AiThetaStreamerPtr& streamer, bool createIfNotExist)
{
    {
        autil::ScopedReadLock lock(_streamerMapLock);
        auto iterator = _streamerMap.find(indexId);
        if (iterator != _streamerMap.end()) {
            streamer = iterator->second;
            return true;
        } else if (!createIfNotExist) {
            AUTIL_LOG(DEBUG, "realtime streamer[%ld] not exist", indexId);
            return false;
        }
    }

    AUTIL_LOG(INFO, "index streamer[%ld] not exist, create it", indexId);
    ANN_CHECK(
        AiThetaFactoryWrapper::CreateStreamer(_indexConfig, std::shared_ptr<RealtimeIndexBuildResource>(), streamer),
        "create streamer failed");
    autil::ScopedWriteLock lock(_streamerMapLock);
    _streamerMap.emplace(indexId, streamer);
    return true;
}

bool RealtimeSegment::DumpIndexData()
{
    auto segDataWriter = GetSegmentDataWriter();
    ANN_CHECK(segDataWriter != nullptr, "get segment data writer failed");
    ANN_CHECK(!segDataWriter->IsClosed(), "segment data writer is closed");
    for (const auto& [indexId, streamer] : _streamerMap) {
        auto indexDataWriter = segDataWriter->GetIndexDataWriter(indexId);
        ANN_CHECK(indexDataWriter != nullptr, "create index data writer for[%ld] failed", indexId);
        ANN_CHECK_OK(CustomizedAiThetaDumper::dump(streamer, indexDataWriter), "dump failed");
    }
    return true;
}

bool RealtimeSegment::EndDump()
{
    auto segDataWriter = GetSegmentDataWriter();
    if (segDataWriter != nullptr) {
        return segDataWriter->Close();
    }
    return true;
}

bool RealtimeSegment::DumpSegmentMeta()
{
    ANN_CHECK(FillSegmentMeta(), "fill segment meta failed");
    return _segmentMeta.Dump(_directory);
}

bool RealtimeSegment::FillSegmentMeta()
{
    auto segDataWriter = GetSegmentDataWriter();
    ANN_CHECK(segDataWriter != nullptr, "get segment data writer failed");
    ANN_CHECK(!segDataWriter->IsClosed(), "segment data writer is closed");
    _segmentMeta.SetSegmentSize(segDataWriter->GetSegmentSize());

    autil::ScopedReadLock lock(_streamerMapLock);
    for (const auto& [indexId, streamer] : _streamerMap) {
        IndexMeta indexMeta;
        indexMeta.docCount = streamer->stats().added_count();
        indexMeta.builderName = streamer->meta().streamer_name();
        if (indexMeta.builderName == OSWG_STREAMER) {
            indexMeta.searcherName = HNSW_SEARCHER;
        } else if (indexMeta.builderName == QC_STREAMER) {
            indexMeta.searcherName = QC_SEARCHER;
        }
        ANN_CHECK(_segmentMeta.AddIndexMeta(indexId, indexMeta), "add indexMeta failed");
    }

    return true;
}

bool RealtimeSegment::Close()
{
    for (auto& [indexId, streamer] : _streamerMap) {
        ANN_CHECK_OK(streamer->cleanup(), "streamer[%ld] cleanup failed", indexId);
    }
    _streamerMap.clear();
    return Segment::Close();
}

AUTIL_LOG_SETUP(indexlib.index, RealtimeSegment);
} // namespace indexlibv2::index::ann
