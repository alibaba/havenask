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

#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuilder.h"

#include "indexlib/index/ann/aitheta2/util/PkDumper.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"
using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {

RealtimeSegmentBuilder::RealtimeSegmentBuilder(const AithetaIndexConfig& indexConfig, const std::string& indexName,
                                               const MetricReporterPtr& reporter)
    : SegmentBuilder(indexConfig, indexName, reporter)
    , _buildContextHolder(new AiThetaContextHolder)
    , _buildMemoryUse(0)
{
}

RealtimeSegmentBuilder::~RealtimeSegmentBuilder() {}

bool RealtimeSegmentBuilder::Init(const SegmentBuildResourcePtr& segmentBuildResource)
{
    InitBuildMetrics();

    ANN_CHECK(ParamsInitializer::InitAiThetaMeta(_indexConfig, _indexMeta), "init meta failed");
    _segment = make_shared<RealtimeSegment>(_indexConfig);
    ANN_CHECK(_segment->Open(segmentBuildResource), "open segment failed");

    AUTIL_LOG(INFO, "realtime segment builder is initialized");
    return true;
}

bool RealtimeSegmentBuilder::Build(const IndexFields& data)
{
    ScopedLatencyReporter reporter(_buildLatencyMetric);

    if (_pkHolder.IsExist(data.pk)) {
        ANN_CHECK(DoDelete(data.pk), "delete pk failed");
    }
    ANN_CHECK(DoBuild(data), "build data failed");

    _buildMemoryUse = EstimateBuildMemoryUse();
    METRIC_REPORT(_buildMemoryMetric, _buildMemoryUse);
    return true;
}

bool RealtimeSegmentBuilder::DoDelete(int64_t pk)
{
    docid_t docId = INVALID_DOCID;
    vector<index_id_t> indexIds;
    _pkHolder.Remove(pk, docId, indexIds);
    for (index_id_t indexId : indexIds) {
        auto iterator = _indexBuilderMap.find(indexId);
        ANN_CHECK(iterator != _indexBuilderMap.end(), "index builder with id[%ld] not exist", indexId);
        ANN_CHECK(iterator->second->Delete(docId), "delete docId[%d] pk[%ld] failed", docId, pk);
        QPS_REPORT(_deleteQpsMetric);
    }
    return true;
}

bool RealtimeSegmentBuilder::DoBuild(const IndexFields& data)
{
    _pkHolder.Add(data.pk, data.docId, data.indexIds);
    for (index_id_t indexId : data.indexIds) {
        auto indexBuilder = GetRealtimeIndexBuilder(indexId);
        ANN_CHECK(indexBuilder, "get builder failed. build pk[%ld] docId[%d] failed", data.pk, data.docId);
        ANN_CHECK(indexBuilder->Build(data.docId, data.embedding), "build pk[%ld] failed", data.pk);
        QPS_REPORT(_buildQpsMetric);
    }
    return true;
}

bool RealtimeSegmentBuilder::Dump(const indexlib::file_system::DirectoryPtr& dir,
                                  const std::vector<docid_t>* old2NewDocId)
{
    // TODO(peaker.lgf): realtime build support sort dump
    if (nullptr != old2NewDocId) {
        AUTIL_LOG(ERROR, "current realtime build not support sort dump ");
        return false;
    }
    ScopedLatencyReporter reporter(_dumpLatencyMetric);

    _segment->SetDirectory(dir);
    ANN_CHECK(DumpPrimaryKey(_pkHolder, *_segment), "dump pkey failed");
    ANN_CHECK(_segment->DumpIndexData(), "dump index failed");
    ANN_CHECK(_segment->DumpSegmentMeta(), "dump meta failed");
    ANN_CHECK(_segment->EndDump(), "end dump failed");
    // Never close segment after dump as the segment is still serving!!!
    AUTIL_LOG(INFO, "realtime segment dumps to[%s]", dir->DebugString().c_str());
    return true;
}

size_t RealtimeSegmentBuilder::EstimateBuildMemoryUse()
{
    size_t buildMemoryUse = 0;
    for (const auto& [_, builder] : _indexBuilderMap) {
        buildMemoryUse += builder->GetIndexSize();
    }
    return buildMemoryUse;
}

RealtimeIndexBuilderPtr RealtimeSegmentBuilder::GetRealtimeIndexBuilder(index_id_t indexId)
{
    RealtimeIndexBuilderPtr& builder = _indexBuilderMap[indexId];
    if (builder) {
        return builder;
    }

    AiThetaStreamerPtr streamer;
    if (!_segment->GetRealtimeIndex(indexId, streamer, true)) {
        AUTIL_LOG(ERROR, "get realtime index[%ld] failed", indexId);
        return nullptr;
    }

    IndexQueryMeta queryMeta;
    queryMeta.set_meta(_indexMeta.type(), _indexMeta.dimension());
    builder = make_shared<RealtimeIndexBuilder>(streamer, queryMeta, _buildContextHolder);
    return builder;
}

void RealtimeSegmentBuilder::InitBuildMetrics()
{
    METRIC_SETUP(_buildLatencyMetric, "indexlib.vector.rt_build_latency", kmonitor::GAUGE);
    METRIC_SETUP(_dumpLatencyMetric, "indexlib.vector.rt_dump_latency", kmonitor::GAUGE);
    METRIC_SETUP(_buildMemoryMetric, "indexlib.vector.rt_build_memory", kmonitor::GAUGE);
    METRIC_SETUP(_buildQpsMetric, "indexlib.vector.rt_build_qps", kmonitor::QPS);
    METRIC_SETUP(_deleteQpsMetric, "indexlib.vector.rt_delete_qps", kmonitor::QPS);
}

AUTIL_LOG_SETUP(indexlib.index, RealtimeSegmentBuilder);
} // namespace indexlibv2::index::ann
