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
#include "indexlib/index/operation_log/OperationDumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/operation_log/OperationBase.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationDumper);
Status OperationDumper::Init(const OperationBlockVec& opBlocks)
{
    _opBlockVec.assign(opBlocks.begin(), opBlocks.end());
    // last building operation block may be empty, and has no corresponding block meta
    if (_opBlockVec.back()->Size() == 0) {
        _opBlockVec.pop_back();
    }

    if (_opBlockVec.size() != _operationMeta.GetBlockMetaVec().size()) {
        AUTIL_LOG(ERROR, "operation block size [%lu] not equal to block meta vector[%lu]", _opBlockVec.size(),
                  _operationMeta.GetBlockMetaVec().size());
        return Status::Corruption("operation block size not equal to block meta vector");
    }
    return CheckOperationBlocks();
}

Status OperationDumper::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool)
{
    AUTIL_LOG(INFO, "Begin dump operation to directory: %s", directory->DebugString().c_str());
    // dump data file
    auto [fileStatue, dataFileWriter] =
        directory->GetIDirectory()
            ->CreateFileWriter(OPERATION_LOG_DATA_FILE_NAME, file_system::WriterOption())
            .StatusWith();
    if (!dataFileWriter->ReserveFile(_operationMeta.GetTotalDumpSize()).OK()) {
        AUTIL_LOG(ERROR, "file writer reserve file fail, reserveSize[%lu] file[%s]", _operationMeta.GetTotalDumpSize(),
                  dataFileWriter->GetLogicalPath().c_str());
        return Status::IOError("reserve file writer failed");
    }

    const OperationMeta::BlockMetaVec& blockMetas = _operationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < _opBlockVec.size(); i++) {
        RETURN_IF_STATUS_ERROR(_opBlockVec[i]->Dump(dataFileWriter, blockMetas[i].maxOperationSerializeSize),
                               "dump operation failed");
    }

    size_t actualDumpSize = dataFileWriter->GetLength();
    if (!dataFileWriter->Close().OK()) {
        AUTIL_LOG(ERROR, "file writer close fail, file[%s]", dataFileWriter->GetLogicalPath().c_str());
        return Status::IOError();
    }
    if (actualDumpSize != _operationMeta.GetTotalDumpSize()) {
        AUTIL_LOG(ERROR, "actual dump size[%lu] is not equal with estimated dump size[%lu]", actualDumpSize,
                  _operationMeta.GetTotalDumpSize());
        return Status::Corruption("actual dump size invalid");
    }
    // dump meta file
    auto status =
        directory->GetIDirectory()
            ->Store(OPERATION_LOG_META_FILE_NAME, _operationMeta.ToString(), file_system::WriterOption::AtomicDump())
            .Status();
    RETURN_IF_STATUS_ERROR(status, "store operation log meta file [%s] failed", OPERATION_LOG_META_FILE_NAME.c_str());
    AUTIL_LOG(INFO, "Dump operation data length is %lu bytes, total operation count : %lu.", actualDumpSize,
              _operationMeta.GetOperationCount());
    AUTIL_LOG(INFO, "End dump operation to directory: %s", directory->DebugString().c_str());
    return Status::OK();
}

Status OperationDumper::CheckOperationBlocks() const
{
    const OperationMeta::BlockMetaVec& blockMetaVec = _operationMeta.GetBlockMetaVec();
    for (size_t i = 0; i < blockMetaVec.size(); i++) {
        if (blockMetaVec[i].operationCompress != blockMetaVec[0].operationCompress) {
            AUTIL_LOG(ERROR, "operation blocks are not the same compress type!");
            return Status::Corruption("operation blocks are not the same compress type!");
        }
    }
    return Status::OK();
}
size_t OperationDumper::GetDumpSize(OperationBase* op)
{
    return sizeof(uint8_t)    // 1 byte(operation type)
           + sizeof(int64_t)  // 8 byte timestamp
           + sizeof(uint16_t) // 1 byte hash id
           + op->GetSerializeSize();
}

} // namespace indexlib::index
