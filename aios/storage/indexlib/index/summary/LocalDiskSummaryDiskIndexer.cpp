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
#include "indexlib/index/summary/LocalDiskSummaryDiskIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SummaryGroupFormatter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, LocalDiskSummaryDiskIndexer);
LocalDiskSummaryDiskIndexer::LocalDiskSummaryDiskIndexer(const DiskIndexerParameter& indexerParam)
    : _indexerParam(indexerParam)
{
}

std::pair<Status, std::shared_ptr<VarLenDataReader>> LocalDiskSummaryDiskIndexer::GetSummaryVarLenDataReader(
    int64_t docCount, const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory, bool isOnline)
{
    auto param = VarLenDataParamHelper::MakeParamForSummary(summaryGroupConfig);
    auto fileCompressConfig = summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig();
    if (param.dataCompressorName.empty() && fileCompressConfig) {
        param.dataCompressorName = "uncertain";
    }
    auto dataReader = std::make_shared<VarLenDataReader>(param, isOnline);
    assert(summaryDirectory != nullptr);
    auto [status, summaryGroupDir] = GetSummaryDirectory(summaryGroupConfig, summaryDirectory);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get summary directory fail.");
    RETURN2_IF_STATUS_ERROR(
        dataReader->Init(docCount, summaryGroupDir, SUMMARY_OFFSET_FILE_NAME, SUMMARY_DATA_FILE_NAME), nullptr,
        "init data reader fail in summary indexer");
    return {Status::OK(), dataReader};
}

Status
LocalDiskSummaryDiskIndexer::Open(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                                  const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory)
{
    _summaryGroupConfig = summaryGroupConfig;
    if (!_summaryGroupConfig->NeedStoreSummary()) {
        return Status::OK(); // in this case, need data exist in summary index format
    }
    auto [status, dataReader] =
        GetSummaryVarLenDataReader(_indexerParam.docCount, _summaryGroupConfig, summaryDirectory, true);
    if (status.IsOK()) {
        _dataReader = dataReader;
    }
    return status;
}

size_t LocalDiskSummaryDiskIndexer::EstimateMemUsed(
    const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory)
{
    assert(summaryGroupConfig != nullptr);
    if (!summaryGroupConfig->NeedStoreSummary()) {
        return 0; // in this case, need data exist in summary index format
    }
    auto [status, summaryGroupDir] = GetSummaryDirectory(summaryGroupConfig, summaryDirectory);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get summary directory fail.");
        return 0;
    }
    auto [dataFileStatus, dataFileSize] =
        summaryGroupDir->EstimateFileMemoryUse(SUMMARY_DATA_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG)
            .StatusWith();
    if (!dataFileStatus.IsOK()) {
        AUTIL_LOG(ERROR, "fail to estimate file memory use for data file in summary indexer");
        return 0;
    }
    auto [offsetFileStatus, offsetFileSize] =
        summaryGroupDir->EstimateFileMemoryUse(SUMMARY_OFFSET_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG)
            .StatusWith();
    if (!dataFileStatus.IsOK()) {
        AUTIL_LOG(ERROR, "fail to estimate file memory use for offset file in summary indexer");
        return 0;
    }
    return dataFileSize + offsetFileSize;
}

size_t LocalDiskSummaryDiskIndexer::EvaluateCurrentMemUsed()
{
    if (!_summaryGroupConfig->NeedStoreSummary()) {
        return 0; // in this case, need data exist in summary index format
    }
    assert(_dataReader != nullptr);
    return _dataReader->EvaluateCurrentMemUsed();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>> LocalDiskSummaryDiskIndexer::GetSummaryDirectory(
    const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory)
{
    assert(summaryGroupConfig != nullptr);
    assert(summaryDirectory != nullptr);

    if (!summaryGroupConfig->IsDefaultGroup()) {
        const auto& groupName = summaryGroupConfig->GetGroupName();
        auto [status, directory] = summaryDirectory->GetDirectory(groupName).StatusWith();
        RETURN2_IF_STATUS_ERROR(status, nullptr, "get group directory fail. group name = [%s]", groupName.c_str());
        return std::make_pair(Status::OK(), directory);
    }
    return std::make_pair(Status::OK(), summaryDirectory);
}

std::pair<Status, bool>
LocalDiskSummaryDiskIndexer::GetDocument(docid_t localDocId,
                                         indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    assert(localDocId != INVALID_DOCID);
    autil::StringView data;
    if (!_summaryGroupConfig->NeedStoreSummary()) {
        return std::make_pair(Status::OK(), true);
    }
    auto [status, ret] = _dataReader->GetValue(localDocId, data, summaryDoc->getPool());
    RETURN2_IF_STATUS_ERROR(status, false, "get data from file fail for doc [%d]", localDocId);
    if (!ret) {
        return std::make_pair(Status::OK(), false);
    }

    char* value = (char*)data.data();
    uint32_t len = data.size();
    indexlib::document::SummaryGroupFormatter formatter(_summaryGroupConfig);
    ret = formatter.DeserializeSummary(summaryDoc, value, len);
    if (ret) {
        return std::make_pair(Status::OK(), true);
    }
    return std::make_pair(Status::InternalError("Deserialize summary[docid = %d] FAILED.", localDocId), false);
}

future_lite::coro::Lazy<std::vector<indexlib::index::ErrorCode>>
LocalDiskSummaryDiskIndexer::GetDocument(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                                         indexlib::file_system::ReadOption readOption,
                                         const SearchSummaryDocVec* docs) noexcept
{
    std::vector<indexlib::index::ErrorCode> ec(docIds.size(), indexlib::index::ErrorCode::OK);
    if (!_summaryGroupConfig->NeedStoreSummary()) {
        co_return ec;
    }
    assert(docs);
    assert(docIds.size() == docs->size());
    std::vector<autil::StringView> datas;
    auto ret = co_await _dataReader->GetValue(docIds, sessionPool, readOption, &datas);

    assert(ret.size() == docIds.size());
    for (size_t i = 0; i < ret.size(); ++i) {
        if (indexlib::index::ErrorCode::OK != ret[i]) {
            ec[i] = ret[i];
            continue;
        }
        char* value = (char*)datas[i].data();
        uint32_t len = datas[i].size();
        indexlib::document::SummaryGroupFormatter formatter(_summaryGroupConfig);
        indexlib::document::SearchSummaryDocument* summaryDoc = (*docs)[i];
        if (!formatter.DeserializeSummary(summaryDoc, value, len)) {
            ec[i] = indexlib::index::ErrorCode::Runtime;
            AUTIL_LOG(ERROR, "Deserialize summary[docid = %d] FAILED.", docIds[i]);
            continue;
        }
    }
    co_return ec;
}

} // namespace indexlibv2::index
