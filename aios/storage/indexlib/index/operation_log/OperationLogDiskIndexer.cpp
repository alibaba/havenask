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
#include "indexlib/index/operation_log/OperationLogDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/operation_log/BatchOpLogIterator.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/operation_log/NormalSegmentOperationIterator.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/OperationMeta.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationLogDiskIndexer);

OperationLogDiskIndexer::OperationLogDiskIndexer(segmentid_t segmentid) : _segmentid(segmentid) {}

OperationLogDiskIndexer::~OperationLogDiskIndexer() {}

Status OperationLogDiskIndexer::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                     const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    _directory = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory);
    _opConfig = std::dynamic_pointer_cast<OperationLogConfig>(indexConfig);
    if (_directory->IsExist(OPERATION_LOG_META_FILE_NAME)) {
        _opMeta.reset(new OperationMeta);
        std::string metaStr;
        auto status = _directory->GetIDirectory()
                          ->Load(OPERATION_LOG_META_FILE_NAME,
                                 file_system::ReaderOption::PutIntoCache(file_system::FSOT_MEM), metaStr)
                          .Status();
        RETURN_IF_STATUS_ERROR(status, "load operation meta file for segment [%d] failed", _segmentid);
        _opMeta->InitFromString(metaStr);
    }
    auto status = OperationFieldInfo::GetLatestOperationFieldInfo(indexDirectory, _operationFieldInfo);
    RETURN_IF_STATUS_ERROR(status, "load operation field info for segment [%d] failed", _segmentid);
    if (!_operationFieldInfo) {
        _operationFieldInfo.reset(new OperationFieldInfo());
        _operationFieldInfo->Init(_opConfig);
    }
    return Status::OK();
}

size_t OperationLogDiskIndexer::EstimateMemUsed(const std::shared_ptr<IIndexConfig>& indexConfig,
                                                const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    return 0;
}

segmentid_t OperationLogDiskIndexer::GetSegmentId() const { return _segmentid; }
size_t OperationLogDiskIndexer::EvaluateCurrentMemUsed() { return 0; }
bool OperationLogDiskIndexer::GetOperationMeta(OperationMeta& operationMeta) const
{
    if (_opMeta) {
        operationMeta = *_opMeta;
        return true;
    }
    return false;
}

std::pair<Status, std::shared_ptr<SegmentOperationIterator>>
OperationLogDiskIndexer::CreateSegmentOperationIterator(size_t offset, const indexlibv2::framework::Locator& locator)
{
    if (_opMeta == nullptr) {
        return {Status::OK(), nullptr};
    }
    OperationMeta opMeta(*_opMeta);
    std::shared_ptr<NormalSegmentOperationIterator> iter(
        new NormalSegmentOperationIterator(_opConfig, _operationFieldInfo, opMeta, _segmentid, offset, locator));
    auto status = iter->Init(_directory);
    RETURN2_IF_STATUS_ERROR(status, nullptr,
                            "create segment operation iterator failed, segmentId [%d], offset [%lu], locator [%s]",
                            _segmentid, offset, locator.DebugString().c_str());
    return {status, iter};
}

std::pair<Status, std::shared_ptr<BatchOpLogIterator>> OperationLogDiskIndexer::CreateBatchIterator()
{
    if (_opMeta == nullptr) {
        return {Status::OK(), nullptr};
    }
    OperationMeta opMeta(*_opMeta);
    auto batchIter = std::make_shared<BatchOpLogIterator>();
    batchIter->Init(_opConfig, _operationFieldInfo, opMeta, _directory->GetIDirectory());
    return {Status::OK(), batchIter};
}

bool OperationLogDiskIndexer::IsSealed() const { return true; }
} // namespace indexlib::index
