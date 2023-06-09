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
#include "indexlib/index/operation_log/NormalSegmentOperationIterator.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/operation_log/FileOperationBlock.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, NormalSegmentOperationIterator);

NormalSegmentOperationIterator::NormalSegmentOperationIterator(
    const std::shared_ptr<OperationLogConfig>& indexConfig,
    const std::shared_ptr<OperationFieldInfo>& operationFieldInfo, const OperationMeta& operationMeta,
    segmentid_t segmentId, size_t offset, const indexlibv2::framework::Locator& locator)
    : InMemSegmentOperationIterator(indexConfig, operationFieldInfo, operationMeta, segmentId, offset, locator)
{
}

Status NormalSegmentOperationIterator::Init(const std::shared_ptr<file_system::Directory>& operationDirectory)
{
    if (_opMeta.GetOperationCount() == 0 || _beginPos > _opMeta.GetOperationCount() - 1) {
        AUTIL_LOG(INFO, "beginPos [%lu] is not valid, operation count is [%lu]", _beginPos,
                  _opMeta.GetOperationCount());
        _lastCursor.pos = (int32_t)_opMeta.GetOperationCount() - 1;
        return Status::InvalidArgs("begin pos is not valid");
    }

    auto [status, fileReader] = operationDirectory->GetIDirectory()
                                    ->CreateFileReader(OPERATION_LOG_DATA_FILE_NAME, file_system::FSOT_MMAP)
                                    .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create file reader failed");
    _fileReader = fileReader;

    assert(_fileReader);
    OperationBlockVec opBlockVec;
    const OperationMeta::BlockMetaVec& blockMeta = _opMeta.GetBlockMetaVec();
    size_t beginOffset = 0;
    for (size_t i = 0; i < blockMeta.size(); ++i) {
        std::shared_ptr<FileOperationBlock> opBlock(new FileOperationBlock());
        opBlock->Init(_fileReader, beginOffset, blockMeta[i]);
        beginOffset += blockMeta[i].dumpSize;
        opBlockVec.push_back(opBlock);
    }
    return InMemSegmentOperationIterator::Init(opBlockVec);
}

NormalSegmentOperationIterator::~NormalSegmentOperationIterator() {}

} // namespace indexlib::index
