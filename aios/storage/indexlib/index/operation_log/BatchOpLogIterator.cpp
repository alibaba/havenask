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
#include "indexlib/index/operation_log/BatchOpLogIterator.h"

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/operation_log/FileOperationBlock.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, BatchOpLogIterator);

BatchOpLogIterator::BatchOpLogIterator() {}

BatchOpLogIterator::~BatchOpLogIterator() {}

void BatchOpLogIterator::Init(const std::shared_ptr<OperationLogConfig>& opConfig,
                              const std::shared_ptr<OperationFieldInfo>& opFieldInfo, const OperationMeta& opMeta,
                              const std::shared_ptr<file_system::IDirectory>& operationDirectory)
{
    _opFactory.Init(opConfig);
    _opMeta = opMeta;
    _opFieldInfo = opFieldInfo;
    _directory = operationDirectory;
}
std::pair<Status, std::vector<OperationBase*>> BatchOpLogIterator::LoadOperations(size_t threadNum)
{
    auto startTime = autil::TimeUtility::currentTimeInMicroSeconds();
    std::vector<OperationBase*> ret;
    std::vector<autil::ThreadPtr> threads;
    auto totalBlockNum = _opMeta.GetBlockMetaVec().size();
    threadNum = std::min(totalBlockNum, threadNum);
    AUTIL_LOG(INFO, "start load op log  blockNum[%lu], threadNum[%lu]", totalBlockNum, threadNum);
    if (totalBlockNum == 0) {
        return {Status::OK(), ret};
    }
    std::vector<Status> resultStatus(threadNum, Status::Uninitialize());
    std::vector<std::vector<std::shared_ptr<OperationBlock>>> resultOps(threadNum);
    size_t mod = totalBlockNum % threadNum;
    size_t beginIdx = 0;
    auto avgBlockNum = totalBlockNum / threadNum;
    for (size_t i = 0; i < threadNum; ++i) {
        size_t blockNum = avgBlockNum;
        if (i < mod) {
            blockNum++;
        }
        auto thread = autil::Thread::createThread(std::bind(&BatchOpLogIterator::SingleThreadLoadOperations, this,
                                                            beginIdx, blockNum, &resultStatus[i], &resultOps[i]),
                                                  "batch_oplog_iterator");
        if (!thread) {
            AUTIL_LOG(ERROR, "create thread failed, load operation failed");
            return {Status::Corruption(), ret};
        }
        threads.push_back(thread);
        beginIdx += blockNum;
    }
    if (beginIdx != totalBlockNum) {
        AUTIL_LOG(ERROR, "unexpected error triggered, beginIdx [%lu] != totalBlockNum [%lud]", beginIdx, totalBlockNum);
        return {Status::Unknown(), ret};
    }

    threads.clear();
    for (size_t i = 0; i < threadNum; ++i) {
        if (!resultStatus[i].IsOK()) {
            AUTIL_LOG(ERROR, "process operation has error");
            return {resultStatus[i], ret};
        }
        _blocks.insert(_blocks.end(), resultOps[i].begin(), resultOps[i].end());
    }
    for (size_t i = 0; i < totalBlockNum; ++i) {
        const auto& operations = _blocks[i]->GetOperations();
        for (size_t j = 0; j < operations.Size(); ++j) {
            if (operations[j]) {
                operations[j]->SetOperationFieldInfo(_opFieldInfo);
            }
            ret.push_back(operations[j]);
        }
    }
    auto loadTime = autil::TimeUtility::currentTimeInMicroSeconds() - startTime;
    AUTIL_LOG(INFO, "end load op log time [%lu], total operation count [%lu]", loadTime, ret.size());
    return {Status::OK(), ret};
}

void BatchOpLogIterator::SingleThreadLoadOperations(size_t beginBlockIdx, size_t blockNum, Status* resultStatus,
                                                    std::vector<std::shared_ptr<OperationBlock>>* resultOps)
{
    AUTIL_LOG(INFO, "begin load partial operation, begin block idx[%lu], block Num [%lu]", beginBlockIdx, blockNum);
    auto [status, fileReader] =
        _directory->CreateFileReader(OPERATION_LOG_DATA_FILE_NAME, file_system::FSOT_MMAP).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create file reader failed");
        *resultStatus = status;
        return;
    }

    size_t beginOffset = 0;
    const OperationMeta::BlockMetaVec& blockMeta = _opMeta.GetBlockMetaVec();
    for (size_t i = 0; i < beginBlockIdx; ++i) {
        beginOffset += blockMeta[i].dumpSize;
    }
    for (size_t i = beginBlockIdx; i < beginBlockIdx + blockNum; ++i) {
        std::shared_ptr<FileOperationBlock> opBlock(new FileOperationBlock());
        opBlock->Init(fileReader, beginOffset, blockMeta[i]);
        beginOffset += blockMeta[i].dumpSize;
        auto [status, block] = opBlock->CreateOperationBlockForRead(_opFactory);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create operation block failed");
            *resultStatus = status;
            resultOps->clear();
            return;
        }
        resultOps->push_back(block);
    }
    AUTIL_LOG(INFO, "end load partial operation, begin block idx[%lu], block Num [%lu], operation count [%lu]",
              beginBlockIdx, blockNum, resultOps->size());
    *resultStatus = Status::OK();
}

} // namespace indexlib::index
