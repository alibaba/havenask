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
#include "indexlib/index/summary/SummaryMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/LocalDiskSummaryMerger.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryMerger);

Status SummaryMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::map<std::string, std::any>& params)
{
    _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(indexConfig);
    assert(_summaryIndexConfig != nullptr);
    if (!_summaryIndexConfig->NeedStoreSummary()) {
        return Status::OK();
    }

    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "could't find doc mapper, type [%s]", DocMapper::GetDocMapperType().c_str());
        return Status::Corruption();
    }
    _docMapperName = std::any_cast<std::string>(iter->second);
    for (summarygroupid_t id = 0; id < _summaryIndexConfig->GetSummaryGroupConfigCount(); ++id) {
        const auto& summaryGroupConfig = _summaryIndexConfig->GetSummaryGroupConfig(id);
        if (!summaryGroupConfig->NeedStoreSummary()) {
            continue;
        }
        _groupSummaryMergers[id] = std::make_shared<LocalDiskSummaryMerger>(_summaryIndexConfig, id);
    }
    return Status::OK();
}

Status SummaryMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                            const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    if (!_summaryIndexConfig->NeedStoreSummary()) {
        AUTIL_LOG(DEBUG, "do nothing for summary merger because of summary configuration [!NeedStoreSummary]");
        return Status::OK();
    }
    std::stringstream ss;
    std::for_each(segMergeInfos.targetSegments.begin(), segMergeInfos.targetSegments.end(),
                  [&ss, delim = ""](const std::shared_ptr<framework::SegmentMeta>& segMeta) {
                      ss << delim
                         << indexlib::util::PathUtil::JoinPath(segMeta->segmentDir->DebugString(), SUMMARY_INDEX_PATH);
                  });
    AUTIL_LOG(DEBUG, "Start merge segments, dir : %s", ss.str().c_str());
    std::shared_ptr<DocMapper> docMapper;
    auto status =
        taskResourceManager->LoadResource<DocMapper>(/*name=*/_docMapperName,
                                                     /*resourceType=*/DocMapper::GetDocMapperType(), docMapper);
    RETURN_IF_STATUS_ERROR(status, "load doc mappaer failed");
    status = DoMerge(segMergeInfos, docMapper);
    RETURN_IF_STATUS_ERROR(status, "do summary merge operation fail");
    AUTIL_LOG(DEBUG, "Finish sort by weight merging to dir : %s", ss.str().c_str());
    return Status::OK();
}

Status SummaryMerger::DoMerge(const SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper)
{
    for (auto& [groupId, localDiskSummaryMerger] : _groupSummaryMergers) {
        auto status = localDiskSummaryMerger->Merge(segMergeInfos, docMapper);
        RETURN_IF_STATUS_ERROR(status, "fail to do merge operation for summary group [%d]", groupId);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
