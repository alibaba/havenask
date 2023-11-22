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
#include "indexlib/index/ann/aitheta2/AithetaIndexMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/ann/aitheta2/impl/CustomizedAithetaLogger.h"
#include "indexlib/index/ann/aitheta2/impl/DocMapperWrapper.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentMerger.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingAttrSegment.h"
#include "indexlib/index/attribute/Common.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, AithetaIndexMerger);

Status AithetaIndexMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                const map<string, any>& params)
{
    CustomizedAiThetaLogger::RegisterIndexLogger();
    _indexName = indexConfig->GetIndexName();
    _indexConfig = std::dynamic_pointer_cast<config::ANNIndexConfig>(indexConfig);
    if (_indexConfig == nullptr) {
        AUTIL_LOG(ERROR, "indexlibv2::config::InvertedIndexConfig failed to dynamic cast to ANNIndexConfig [%s][%s]",
                  _indexName.c_str(), indexConfig->GetIndexType().c_str());
        return Status::InvalidArgs();
    }
    _aithetaIndexConfig = AithetaIndexConfig(_indexConfig->GetParameters());
    const auto& fieldConfigs = _indexConfig->GetFieldConfigVector();
    if (fieldConfigs.size() < 2) {
        AUTIL_LOG(ERROR, "field count < 2 in index config");
        return Status::InvalidArgs();
    }
    // embedding field name is set in the back of config
    _embAttrName = fieldConfigs.back()->GetFieldName();

    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "not find doc mapper name by type [%s]", DocMapper::GetDocMapperType().c_str());
        return Status::Corruption("not find doc mapper name by type");
    }
    _docMapperName = std::any_cast<std::string>(iter->second);
    return Status::OK();
}

void AithetaIndexMerger::InitMetricReporter(const indexlib::util::MetricProviderPtr& provider)
{
    if (provider) {
        _metricReporter = std::make_shared<MetricReporter>(provider, _indexName);
        _mergeLatencyMetric = _metricReporter->Declare("indexlib.vector.dump_latency", kmonitor::GAUGE);
    }
}

Status AithetaIndexMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (segMergeInfos.targetSegments.size() != 1) {
        AUTIL_LOG(ERROR, "target segments size is not 1, cannot merge");
        return Status::InvalidArgs();
    }
    ScopedLatencyReporter reporter(_mergeLatencyMetric);

    std::shared_ptr<DocMapper> docMapper;
    auto status = taskResourceManager->LoadResource(_docMapperName, DocMapper::GetDocMapperType(), docMapper);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load doc mapper fail");
        return status;
    }
    auto targetIndexDir = PrepareTargetIndexDirectory(segMergeInfos.targetSegments[0]->segmentDir);
    return DoMerge(segMergeInfos.srcSegments, targetIndexDir, docMapper);
}

Status AithetaIndexMerger::DoMerge(const std::vector<SourceSegment>& srcSegments, std::shared_ptr<Directory>& targetDir,
                                   const std::shared_ptr<DocMapper>& docMapper)
{
    vector<shared_ptr<NormalSegment>> normalSegments;
    vector<shared_ptr<EmbeddingAttrSegment>> embeddingAttrSegments;
    for (const SourceSegment& srcSegment : srcSegments) {
        segmentid_t segmentId = srcSegment.segment->GetSegmentId();
        auto segmentDir = srcSegment.segment->GetSegmentDirectory();
        if (nullptr == segmentDir) {
            AUTIL_LOG(WARN, "cannot find segment dir of segment[%d]", segmentId);
            continue;
        }
        auto annIndexDir = GetAithetaIndexDir(segmentDir);
        if (nullptr == annIndexDir) {
            AUTIL_LOG(WARN, "ann index dir of segment [%d] is nullptr.", segmentId);
            continue;
        }
        auto normalSegment = make_shared<NormalSegment>(annIndexDir, AithetaIndexConfig(), false);
        if (!normalSegment->Open()) {
            AUTIL_LOG(WARN, "open normal segment with directory[%s] failed.", annIndexDir->DebugString().c_str());
            return Status::InternalError("open normal segment with directory[%s] failed.",
                                         annIndexDir->DebugString().c_str());
        }
        auto docMapWrapper = std::make_shared<DocMapperWrapper>(docMapper, srcSegment.baseDocid);
        normalSegment->SetDocMapWrapper(docMapWrapper);
        normalSegments.push_back(normalSegment);
    }
    NormalSegmentMerger segmentMerger(_aithetaIndexConfig, _indexName, _metricReporter);
    if (!segmentMerger.Init()) {
        return Status::InternalError("init normal segment merger failed");
    }
    // move segments to the segment merger to make sure the resources are freed properly
    if (!segmentMerger.Merge(std::move(normalSegments), std::move(embeddingAttrSegments), nullptr, targetDir)) {
        return Status::InternalError("normal segment merge failed.");
    }
    return Status::OK();
}

std::shared_ptr<Directory> AithetaIndexMerger::GetAithetaIndexDir(const std::shared_ptr<Directory>& segmentDir) const
{
    auto indexDir = segmentDir->GetDirectory(ANN_INDEX_PATH, /*throwExceptionIfNotExist=*/false);
    if (nullptr == indexDir) {
        AUTIL_LOG(WARN, "cannot find [%s] in dir [%s]", ANN_INDEX_PATH.c_str(), segmentDir->DebugString().c_str());
        return nullptr;
    }
    auto annIndexDir = indexDir->GetDirectory(_indexName, /*throwExceptionIfNotExist=*/false);
    if (nullptr == annIndexDir) {
        AUTIL_LOG(WARN, "cannot find [%s] in dir [%s]", _indexName.c_str(), indexDir->DebugString().c_str());
        return nullptr;
    }
    if (!SegmentMeta::IsExist(annIndexDir)) {
        AUTIL_LOG(WARN, "segment meta is not exist in dir [%s]", annIndexDir->DebugString().c_str());
        return nullptr;
    }
    return annIndexDir;
}

std::shared_ptr<Directory>
AithetaIndexMerger::PrepareTargetIndexDirectory(const std::shared_ptr<Directory>& targetSegmentDir) const
{
    auto subDir = targetSegmentDir->MakeDirectory(ANN_INDEX_PATH);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    subDir->RemoveDirectory(_indexName, /*mayNonExist=*/removeOption);
    return subDir->MakeDirectory(_indexName);
}

} // namespace indexlibv2::index::ann
