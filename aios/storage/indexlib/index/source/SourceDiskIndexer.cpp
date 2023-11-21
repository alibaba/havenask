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
#include "indexlib/index/source/SourceDiskIndexer.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceFormatter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
using indexlib::document::SerializedSourceDocument;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SourceDiskIndexer);
SourceDiskIndexer::SourceDiskIndexer(const DiskIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}
Status SourceDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    AUTIL_LOG(INFO, "begin open source disk indexer for segment [%d], doc count [%lu]", _indexerParam.segmentId,
              _indexerParam.docCount);
    _sourceIndexConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(indexConfig);
    if (!_sourceIndexConfig) {
        RETURN_STATUS_ERROR(InvalidArgs, "source index config is empty");
    }
    if (_indexerParam.docCount == 0) {
        AUTIL_LOG(INFO, "source disk indexer open done, doc count is 0 for segment [%d]", _indexerParam.segmentId);
        return Status::OK();
    }

    if (_sourceIndexConfig->IsDisabled()) {
        RETURN_STATUS_ERROR(InvalidArgs, "all source field group disabled");
    }
    if (!indexDirectory) {
        RETURN_STATUS_ERROR(InvalidArgs, "index directory is nullptr");
    }

    for (const auto& groupConfig : _sourceIndexConfig->GetGroupConfigs()) {
        sourcegroupid_t groupId = groupConfig->GetGroupId();
        if (groupConfig->IsDisabled()) {
            _groupReaders.push_back(std::shared_ptr<VarLenDataReader>());
            AUTIL_LOG(INFO, "group [%d] is disabled, ignore", groupId);
            continue;
        }
        auto param = VarLenDataParamHelper::MakeParamForSourceData(groupConfig);
        auto fileCompressConfig = groupConfig->GetParameter().GetFileCompressConfigV2();
        if (param.dataCompressorName.empty() && fileCompressConfig) {
            param.dataCompressorName = "uncertain";
        }
        auto groupReader = std::make_shared<VarLenDataReader>(param, /*isOnline*/ true);
        auto [groupSt, groupDir] = indexDirectory->GetDirectory(GetDataDir(groupId)).StatusWith();
        RETURN_IF_STATUS_ERROR(groupSt, "get group directory failed");
        RETURN_IF_STATUS_ERROR(
            groupReader->Init(_indexerParam.docCount, groupDir, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME),
            "init group reader failed");
        _groupReaders.push_back(groupReader);
        AUTIL_LOG(INFO, "init group [%d] done", groupId);
    }

    _metaReader.reset(new VarLenDataReader(VarLenDataParamHelper::MakeParamForSourceMeta(), /*isOnline*/ true));
    auto [metaSt, metaDir] = indexDirectory->GetDirectory(SOURCE_META_DIR).StatusWith();
    RETURN_IF_STATUS_ERROR(metaSt, "get meta dir failed");
    RETURN_IF_STATUS_ERROR(
        _metaReader->Init(_indexerParam.docCount, metaDir, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME),
        "init meta reader failed");
    AUTIL_LOG(INFO, "init source disk indexer for segment [%d] success", _indexerParam.segmentId);
    return Status::OK();
}
size_t SourceDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto sourceConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(indexConfig);
    if (!sourceConfig) {
        return 0;
    }
    size_t totalMemory = 0;
    std::vector<std::string> pathsToEstimate;
    pathsToEstimate.push_back(SOURCE_META_DIR);
    for (size_t i = 0; i < sourceConfig->GetSourceGroupCount(); ++i) {
        if (!sourceConfig->GetGroupConfig(i)->IsDisabled()) {
            pathsToEstimate.push_back(GetDataDir(i));
        }
    }

    for (const auto& path : pathsToEstimate) {
        auto [st, subDir] = indexDirectory->GetDirectory(path).StatusWith();
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "get directory from path [%s] failed", path.c_str());
            return 0;
        }
        std::vector<std::string> files({SOURCE_DATA_FILE_NAME, SOURCE_OFFSET_FILE_NAME});
        for (const auto& file : files) {
            auto [sizeStatus, memoryUse] =
                subDir->EstimateFileMemoryUse(file, indexlib::file_system::FSOT_LOAD_CONFIG).StatusWith();
            if (!sizeStatus.IsOK()) {
                AUTIL_LOG(ERROR, "estimate file [%s] from path [%s] failed", file.c_str(), path.c_str());
                return 0;
            }
            totalMemory += memoryUse;
        }
    }
    return totalMemory;
}
size_t SourceDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t memoryUsed = 0;
    for (const auto& groupReader : _groupReaders) {
        if (groupReader) {
            memoryUsed += groupReader->EvaluateCurrentMemUsed();
        }
    }
    if (_metaReader) {
        memoryUsed += _metaReader->EvaluateCurrentMemUsed();
    }
    return memoryUsed;
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SourceDiskIndexer::GetDocument(const std::vector<docid_t>& docIds,
                               const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                               autil::mem_pool::PoolBase* sessionPool, indexlib::file_system::ReadOption readOption,
                               const std::vector<indexlib::document::SerializedSourceDocument*>* docs) const
{
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    std::vector<future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>> subTasks;
    subTasks.reserve(requiredGroupdIds.size() + 1);
    std::vector<autil::StringView> metaResult;
    std::deque<std::vector<autil::StringView>> dataResults;
    subTasks.push_back(_metaReader->GetValue(docIds, sessionPool, readOption, &metaResult));
    for (auto groupId : requiredGroupdIds) {
        auto& groupReader = _groupReaders[groupId];
        if (!groupReader) {
            continue;
        }
        dataResults.emplace_back(std::vector<autil::StringView>());
        subTasks.push_back(groupReader->GetValue(docIds, sessionPool, readOption, &(dataResults.back())));
    }
    auto subTaskResults = co_await future_lite::coro::collectAll(std::move(subTasks));
    // fill meta result
    assert(!subTaskResults[0].hasError());
    auto& metaEc = subTaskResults[0].value();
    assert(metaEc.size() == docIds.size());
    assert(metaResult.size() == docIds.size());
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (metaEc[i] == indexlib::index::ErrorCode::OK) {
            (*docs)[i]->SetMeta(metaResult[i]);
        } else {
            result[i] = metaEc[i];
        }
    }
    size_t subTaskIdx = 0;

    // fill group data result
    for (auto groupid : requiredGroupdIds) {
        if (!_groupReaders[groupid]) {
            continue;
        }
        assert(subTaskIdx + 1 < subTaskResults.size());
        assert(!subTaskResults[subTaskIdx + 1].hasError());
        auto& ecs = subTaskResults[subTaskIdx + 1].value();

        assert(ecs.size() == docIds.size());
        auto& dataResult = dataResults[subTaskIdx];
        assert(dataResult.size() == docIds.size());
        for (size_t i = 0; i < docIds.size(); ++i) {
            if (ecs[i] == indexlib::index::ErrorCode::OK) {
                (*docs)[i]->SetGroupValue(groupid, dataResult[i]);
            } else if (result[i] == indexlib::index::ErrorCode::OK) {
                result[i] = ecs[i];
            }
        }
        subTaskIdx++;
    }
    co_return result;
}

} // namespace indexlibv2::index
