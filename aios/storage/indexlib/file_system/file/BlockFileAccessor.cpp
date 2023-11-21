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
#include "indexlib/file_system/file/BlockFileAccessor.h"

#include <algorithm>
#include <exception>
#include <functional>
#include <iosfwd>
#include <stdexcept>
#include <string.h>
#include <sys/uio.h>
#include <tuple>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/Diagnostic.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "fslib/common/common_type.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/FutureAwaiter.h"
#include "future_lite/Helper.h"
#include "future_lite/Try.h"
#include "future_lite/Unit.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/Exception.h"

namespace future_lite {
template <typename T>
class MoveWrapper;
template <typename T>
class Promise;
} // namespace future_lite

using namespace std;
using namespace autil;

using future_lite::Future;
using future_lite::makeReadyFuture;
using future_lite::MoveWrapper;
using future_lite::Promise;
using future_lite::Try;
using future_lite::Unit;
using future_lite::coro::Lazy;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileAccessor);

const size_t BlockFileAccessor::DEFAULT_IO_BATCH_SIZE = 4;

FSResult<void> BlockFileAccessor::Open(const string& path, const PackageOpenMeta& packageOpenMeta) noexcept
{
    _fileId = FileBlockCache::GetFileId(_linkRoot + '/' + packageOpenMeta.GetPhysicalFilePath() + "#" + path);
    auto [ec, file] = FslibWrapper::OpenFile(packageOpenMeta.GetPhysicalFilePath(), fslib::READ, _useDirectIO,
                                             packageOpenMeta.GetPhysicalFileLength());
    RETURN_IF_FS_ERROR(ec, "OpenFile [%s] failed", packageOpenMeta.GetPhysicalFilePath().c_str());
    _filePtr = std::move(file);
    _fileLength = packageOpenMeta.GetLength();
    _fileBeginOffset = packageOpenMeta.GetOffset();
    return FSEC_OK;
}

FSResult<void> BlockFileAccessor::Open(const string& path, int64_t fileLength) noexcept
{
    _fileId = FileBlockCache::GetFileId(_linkRoot + '/' + path);
    if (fileLength < 0) {
        auto [ec, length] = FslibWrapper::GetFileLength(path);
        RETURN_IF_FS_ERROR(ec, "GetFileLength [%s] failed", path.c_str());
        _fileLength = length;
    } else {
        _fileLength = fileLength;
    }
    auto [ec, file] = FslibWrapper::OpenFile(path, fslib::READ, _useDirectIO, _fileLength);
    RETURN_IF_FS_ERROR(ec, "OpenFile [%s] failed", path.c_str());
    _filePtr = std::move(file);
    _fileBeginOffset = 0;
    return FSEC_OK;
}

FSResult<void> BlockFileAccessor::Close() noexcept
{
    if (_filePtr) {
        std::string filename = _filePtr->GetFileName();
        auto ec = _filePtr->Close().Code();
        if (ec != ErrorCode::FSEC_OK) {
            AUTIL_LOG(ERROR, "close file failed, file[%s], ec [%d]", filename.c_str(), ec);
            // return FSEC_ERROR; // why no need return error
        }
        _filePtr.reset();
    }
    return FSEC_OK;
}

void BlockFileAccessor::FillBuffer(const util::BlockHandle& handle, ReadContext* ctx) noexcept
{
    assert(ctx);
    size_t inBlockOffset = GetInBlockOffset(ctx->curOffset);
    size_t blockDataSize = handle.GetDataSize();
    size_t curReadBytes = inBlockOffset + ctx->leftLen > blockDataSize ? blockDataSize - inBlockOffset : ctx->leftLen;
    if (ctx->curBuffer) {
        memcpy(ctx->curBuffer, handle.GetData() + inBlockOffset, curReadBytes);
        ctx->curBuffer += curReadBytes;
    }
    ctx->curOffset += curReadBytes;
    ctx->leftLen -= curReadBytes;
}

Future<FSResult<void>> BlockFileAccessor::FillBuffer(size_t startBlockIdx, size_t cnt, const ReadContextPtr& ctx,
                                                     ReadOption option) noexcept
{
    if (cnt == 0) {
        return makeReadyFuture(FSResult<void> {FSEC_OK});
    }

    assert(ctx->curOffset / _blockSize == startBlockIdx);

    return GetBlockHandles(startBlockIdx, cnt, option)
        .thenValue([ctx, this](vector<FSResult<BlockHandle>>&& handles) mutable -> FSResult<void> {
            if (!handles.empty() && !handles[0].OK()) {
                return FSResult<void> {handles[0].Code()};
            }
            for (const auto& handle : handles) {
                assert(handle.OK());
                FillBuffer(handle.Value(), ctx.get());
            }
            return FSResult<void> {FSEC_OK};
        });
}

FSResult<size_t> BlockFileAccessor::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    size_t batchSize = _blockSize * _batchSize;
    size_t readed = 0;
    option.useInternalExecutor = false;
    while (length > 0) {
        size_t readLen = std::min(length, batchSize);
        auto [ec, ret] = DoRead(buffer, readLen, offset, option);
        RETURN2_IF_FS_ERROR(ec, ret, "DoRead [%s] failed, length[%lu], offset[%lu]", _filePtr->GetFileName(), length,
                            offset);
        readed += ret;
        if (buffer) {
            buffer = static_cast<char*>(buffer) + ret;
        }
        length -= ret;
        offset += ret;

        if (ret != readLen) {
            return {FSEC_OK, readed};
        }
    }
    return {FSEC_OK, readed};
}

FSResult<size_t> BlockFileAccessor::DoRead(void* buffer, size_t length, size_t offset,
                                           const ReadOption& option) noexcept
{
    auto ctx = make_shared<ReadContext>(static_cast<uint8_t*>(buffer), length, offset);
    try {
        return ReadFromBlock(ctx, option).get();
    } catch (...) {
        return {FSEC_ERROR, 0};
    }
}

Future<FSResult<size_t>> BlockFileAccessor::ReadAsync(void* buffer, size_t length, size_t offset,
                                                      ReadOption option) noexcept
{
    auto ctx = make_shared<ReadContext>(static_cast<uint8_t*>(buffer), length, offset);
    return ReadFromBlock(ctx, option);
}

FL_LAZY(FSResult<size_t>)
BlockFileAccessor::ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    AddExecutorIfNotSet(&option, FL_COAWAIT FL_CURRENT_EXECUTOR());
    auto ctx = make_shared<ReadContext>(static_cast<uint8_t*>(buffer), length, offset);
    FL_CORETURN FL_COAWAIT ReadFromBlockCoro(ctx, option);
}

size_t BlockFileAccessor::FillOneBlock(const SingleIO& io, Block* block, size_t blockId) const noexcept
{
    if (!block) {
        return 0;
    }
    size_t offsetBegin = max(io.offset, blockId * _blockSize);
    size_t offsetEnd = min(io.offset + io.len, (1 + blockId) * _blockSize);
    size_t copyed = offsetEnd - offsetBegin;
    memcpy((char*)io.buffer + offsetBegin - io.offset, block->data + (offsetBegin % _blockSize), copyed);
    return copyed;
}

future_lite::coro::Lazy<FSResult<size_t>>
BlockFileAccessor::ReadFromFileWrapper(const std::vector<util::Block*>& blocks, size_t beginIdx, size_t endIdx,
                                       size_t offset, int advice, int64_t timeout) noexcept
{
    offset += _fileBeginOffset;
    if (endIdx == beginIdx) {
        co_return co_await _filePtr->PReadAsync(blocks[beginIdx]->data, _blockSize, offset, advice, timeout);
    }
    vector<iovec> iov(endIdx - beginIdx + 1);
    size_t idx = 0;
    for (size_t i = beginIdx; i <= endIdx; ++i) {
        iov[idx].iov_base = blocks[i]->data;
        iov[idx].iov_len = _blockSize;
        idx++;
    }
    co_return co_await _filePtr->PReadVAsync(iov.data(), iov.size(), offset, advice, timeout);
}

Lazy<std::vector<FSResult<util::BlockHandle>>>
BlockFileAccessor::BatchReadBlocksFromFile(const std::vector<size_t>& blockIds, ReadOption option) noexcept
{
    vector<FSResult<util::BlockHandle>> result(blockIds.size());
    vector<Block*> blocks(blockIds.size());
    int64_t timeout = -1;
    if (option.timeoutTerminator) {
        int64_t leftTime = option.timeoutTerminator->getLeftTime();
        if (leftTime > 0) {
            timeout = leftTime;
        } else {
            AUTIL_LOG(ERROR, "read block left time less than 0, timeout, file[%s]", _filePtr->GetFileName());
            for (size_t i = 0; i < result.size(); ++i) {
                result[i].ec = FSEC_OPERATIONTIMEOUT;
            }
            co_return result;
        }
    }

    vector<Lazy<FSResult<size_t>>> subTasks;
    subTasks.reserve(blocks.size());
    for (size_t i = 0; i < blockIds.size(); ++i) {
        auto block = _blockAllocatorPtr->AllocBlock();
        block->id = util::blockid_t(_fileId, blockIds[i]);
        blocks[i] = block;
    }
    size_t lastBid = blockIds[0];
    size_t offset = blockIds[0] * _blockSize;
    size_t beginIdx = 0;
    vector<size_t> batchSize;
    for (size_t i = 1; i < blockIds.size(); ++i) {
        size_t bid = blockIds[i];
        if (bid != lastBid + 1 || i - beginIdx == _batchSize) {
            subTasks.push_back(ReadFromFileWrapper(blocks, beginIdx, i - 1, offset, option.advice, timeout));
            batchSize.push_back(i - beginIdx);
            offset = bid * _blockSize;
            beginIdx = i;
        }
        lastBid = bid;
    }
    subTasks.push_back(ReadFromFileWrapper(blocks, beginIdx, blockIds.size() - 1, offset, option.advice, timeout));
    batchSize.push_back(blockIds.size() - beginIdx);
    int64_t begin = autil::TimeUtility::currentTimeInMicroSeconds();
    auto currentExecutor = co_await future_lite::CurrentExecutor();
    vector<Try<FSResult<size_t>>> readResult;
    if (currentExecutor || !_executor) {
        readResult = co_await future_lite::coro::collectAll(move(subTasks));
    } else {
        readResult = co_await future_lite::coro::collectAll(move(subTasks)).via(_executor);
    }

    int64_t latency = TimeUtility::currentTimeInMicroSeconds() - begin;
    _blockCache->ReportReadLatency(latency, option.blockCounter);
    _blockCache->ReportReadSize(blockIds.size() * _blockSize, option.blockCounter);
    _tagMetricReporter.ReportReadBlockCount(blockIds.size(), option.trace);
    _tagMetricReporter.ReportReadSize(blockIds.size() * _blockSize, option.trace);
    _tagMetricReporter.ReportReadLatency(latency, option.trace);

    if (option.blockCounter != nullptr && option.blockCounter->histCounter != nullptr) {
        option.blockCounter->histCounter->Report(blockIds.size(), blockIds.size() * _blockSize, latency);
    }
    size_t i = 0;
    for (size_t resultIdx = 0; resultIdx < readResult.size(); ++resultIdx) {
        assert(!readResult[resultIdx].hasError());
        auto& singleReadResult = readResult[resultIdx].value();
        for (size_t cnt = 0; cnt < batchSize[resultIdx]; ++cnt) {
            if (singleReadResult.OK()) {
                autil::CacheBase::Handle* cacheHandle = NULL;
                [[maybe_unused]] bool ret = _blockCache->Put(blocks[i], &cacheHandle, _cachePriority);
                assert(ret);
                size_t blockDataSize = _blockSize;
                if (i == readResult.size() - 1 && singleReadResult.result % _blockSize) {
                    blockDataSize = singleReadResult.result % _blockSize;
                }
                result[i] = FSResult<util::BlockHandle>(singleReadResult.ec,
                                                        {_blockCache, cacheHandle, blocks[i], blockDataSize});
            } else {
                _blockAllocatorPtr->FreeBlock(blocks[i]);
                result[i] = FSResult<util::BlockHandle>(singleReadResult.ec, util::BlockHandle());
            }
            i++;
        }
    }
    co_return result;
}

Lazy<vector<FSResult<BlockHandle>>> BlockFileAccessor::GetBlockHandles(const vector<size_t>& blockIdxs,
                                                                       ReadOption option) noexcept
{
    vector<FSResult<BlockHandle>> ret;
    ret.resize(blockIdxs.size());
    vector<size_t> missingBlockIdxs;
    for (size_t i = 0; i < blockIdxs.size(); ++i) {
        size_t blockIdx = blockIdxs[i];
        CacheBase::Handle* cacheHandle = NULL;
        blockid_t blockId(_fileId, blockIdx);
        Block* block = _blockCache->Get(blockId, &cacheHandle);
        if (block) {
            _blockCache->ReportHit(option.blockCounter);
            auto blockDataSize = min(_fileLength - blockIdx * _blockSize, (size_t)_blockSize);
            ret[i] = FSResult<BlockHandle>(FSEC_OK, {_blockCache, cacheHandle, block, blockDataSize});
        } else {
            _blockCache->ReportMiss(option.blockCounter);
            missingBlockIdxs.push_back(blockIdx);
            ret[i].ec = FSEC_ERROR;
        }
    }
    if (!missingBlockIdxs.empty()) {
        auto missingBlockHandles = co_await BatchReadBlocksFromFile(missingBlockIdxs, option);
        size_t resultIdx = 0;
        for (size_t i = 0; i < missingBlockHandles.size(); ++i) {
            while (ret[resultIdx].ec != FSEC_ERROR) {
                resultIdx++;
            }
            swap(ret[resultIdx], missingBlockHandles[i]);
            resultIdx++;
        }
    }
    co_return ret;
}

Lazy<vector<FSResult<size_t>>> BlockFileAccessor::BatchReadOrdered(const BatchIO& batchIO, ReadOption option) noexcept
{
    vector<FSResult<size_t>> result(batchIO.size(), FSResult<size_t>(ErrorCode::FSEC_OK, 0));
    vector<size_t> blockIdxs;
    size_t lastBlockIdx = 0;
    for (auto& singleIO : batchIO) {
        size_t curBlockIdx = GetBlockIdx(singleIO.offset);
        size_t endBlockIdx = GetBlockIdx(singleIO.offset + singleIO.len + _blockSize - 1);
        curBlockIdx = std::max(curBlockIdx, lastBlockIdx);
        for (size_t idx = curBlockIdx; idx < endBlockIdx; ++idx) {
            blockIdxs.push_back(idx);
        }
        if (endBlockIdx > lastBlockIdx) {
            lastBlockIdx = endBlockIdx;
        }
    }

    vector<FSResult<BlockHandle>> cacheBlockHandle = co_await GetBlockHandles(blockIdxs, option);
    FillBatchIOFromHandles(cacheBlockHandle, blockIdxs, batchIO, result);
    co_return result;
}

void BlockFileAccessor::FillBatchIOFromHandles(const vector<FSResult<BlockHandle>>& blockHandles,
                                               const vector<size_t>& blockIdxs, const BatchIO& batchIO,
                                               vector<FSResult<size_t>>& result) const noexcept
{
    for (size_t ioId = 0; ioId < batchIO.size(); ++ioId) {
        auto& singleIO = batchIO[ioId];
        if (result[ioId].result == singleIO.len) {
            continue;
        }
        //[beginIdx, endIdx] is necessary
        FSResult<size_t> ec(ErrorCode::FSEC_OK, 0);

        auto beginIter = std::lower_bound(blockIdxs.begin(), blockIdxs.end(), singleIO.offset / _blockSize);
        size_t endBlockIdx = (singleIO.offset + singleIO.len + _blockSize - 1) / _blockSize;
        while (beginIter != blockIdxs.end() && *beginIter < endBlockIdx) {
            size_t vectorIdx = beginIter - blockIdxs.begin();
            if (blockHandles[vectorIdx].OK()) {
                size_t filled = FillOneBlock(singleIO, blockHandles[vectorIdx].result.GetBlock(), blockIdxs[vectorIdx]);
                result[ioId].result += filled;
            } else {
                ec.ec = blockHandles[vectorIdx].ec;
                break;
            }
            ++beginIter;
        }
        if (!ec.OK()) {
            result[ioId] = ec;
        }
    }
}

Future<FSResult<size_t>> BlockFileAccessor::ReadFromBlock(const ReadContextPtr& ctx, ReadOption option) noexcept
{
    if (ctx->leftLen == 0) {
        return makeReadyFuture(FSResult<size_t> {FSEC_OK, 0ul});
    }

    size_t startOffset = ctx->curOffset;

    // [start, end)
    size_t curBlockIdx = ctx->curOffset / _blockSize;
    size_t missBlockStartIdx = curBlockIdx;
    size_t endBlockIdx = (ctx->curOffset + ctx->leftLen + _blockSize - 1) / _blockSize;
    size_t blockCount = 0;
    std::function<void(const ReadContextPtr)> deferWork;

    bool gotMissBlock = false;
    for (; curBlockIdx < endBlockIdx; ++curBlockIdx) {
        blockid_t blockId(_fileId, curBlockIdx);
        CacheBase::Handle* cacheHandle = nullptr;
        Block* block = _blockCache->Get(blockId, &cacheHandle);
        if (block) {
            auto blockDataSize = min(_fileLength - curBlockIdx * _blockSize, (size_t)_blockSize);
            _blockCache->ReportHit(option.blockCounter);
            _tagMetricReporter.ReportHit(option.trace);
            if (gotMissBlock) {
                deferWork = [this, blockDataSize, cacheHandle, block](const ReadContextPtr& ctx) mutable {
                    FillBuffer(BlockHandle(_blockCache, cacheHandle, block, blockDataSize), ctx.get());
                };
                break;
            }
            FillBuffer(BlockHandle(_blockCache, cacheHandle, block, blockDataSize), ctx.get());
            continue;
        } else {
            if (!gotMissBlock) {
                gotMissBlock = true;
                missBlockStartIdx = curBlockIdx;
            }
            _blockCache->ReportMiss(option.blockCounter);
            _tagMetricReporter.ReportMiss(option.trace);
            blockCount++;
            if (blockCount >= _batchSize) {
                break;
            }
        }
    }

    return FillBuffer(missBlockStartIdx, blockCount, ctx, option)
        .thenValue([this, option, ctx, startOffset, defer = std::move(deferWork)](
                       FSResult<void>&& ret) mutable -> future_lite::Future<FSResult<size_t>> {
            if (defer) {
                defer(ctx);
            }
            RETURN_RESULT_IF_FS_ERROR(ret.Code(), makeReadyFuture(FSResult<size_t> {ret.Code(), 0}),
                                      "FillBuffer failed");
            return ReadFromBlock(ctx, option).thenValue([ctx, startOffset](FSResult<size_t>&& ret) -> FSResult<size_t> {
                return FSResult<size_t> {ret.Code(), ctx->curOffset - startOffset};
            });
        });
}

FL_LAZY(FSResult<size_t>) BlockFileAccessor::ReadFromBlockCoro(const ReadContextPtr& ctx, ReadOption option) noexcept
{
    if (ctx->leftLen == 0) {
        FL_CORETURN FSResult<size_t> {FSEC_OK, 0ul};
    }

    size_t startOffset = ctx->curOffset;

    // [start, end)
    size_t curBlockIdx = ctx->curOffset / _blockSize;
    size_t missBlockStartIdx = curBlockIdx;
    size_t endBlockIdx = (ctx->curOffset + ctx->leftLen + _blockSize - 1) / _blockSize;
    size_t blockCount = 0;
    std::function<void(const ReadContextPtr)> deferWork;

    bool gotMissBlock = false;
    for (; curBlockIdx < endBlockIdx; ++curBlockIdx) {
        blockid_t blockId(_fileId, curBlockIdx);
        CacheBase::Handle* cacheHandle = nullptr;
        Block* block = FL_COAWAIT _blockCache->GetAsync(blockId, &cacheHandle);
        if (block) {
            auto blockDataSize = min(_fileLength - curBlockIdx * _blockSize, (size_t)_blockSize);
            _blockCache->ReportHit(option.blockCounter);
            _tagMetricReporter.ReportHit(option.trace);
            if (gotMissBlock) {
                deferWork = [this, blockDataSize, cacheHandle, block](const ReadContextPtr& ctx) mutable {
                    FillBuffer(BlockHandle(_blockCache, cacheHandle, block, blockDataSize), ctx.get());
                };
                break;
            }
            FillBuffer(BlockHandle(_blockCache, cacheHandle, block, blockDataSize), ctx.get());
            continue;
        } else {
            if (!gotMissBlock) {
                gotMissBlock = true;
                missBlockStartIdx = curBlockIdx;
            }
            _blockCache->ReportMiss(option.blockCounter);
            _tagMetricReporter.ReportMiss(option.trace);
            blockCount++;
            if (blockCount >= _batchSize) {
                break;
            }
        }
    }

    FSResult<void> ret = FL_COAWAIT FillBuffer(missBlockStartIdx, blockCount, ctx, option).toAwaiter();
    FL_CORETURN2_IF_FS_ERROR(ret.Code(), (ctx->curOffset - startOffset), "FillBuffer Failed");
    if (deferWork) {
        deferWork(ctx);
    }
    auto [ec, readLen] = FL_COAWAIT ReadFromBlockCoro(ctx, option);
    (void)readLen;
    if (FSEC_OPERATIONTIMEOUT == ec) {
        FL_CORETURN FSResult<size_t> {FSEC_OPERATIONTIMEOUT, (ctx->curOffset - startOffset)};
    }
    FL_CORETURN2_IF_FS_ERROR(ec, 0ul, "ReadFromBlockCoro failed");
    FL_CORETURN FSResult<size_t> {FSEC_OK, (ctx->curOffset - startOffset)};
}

FSResult<size_t> BlockFileAccessor::Prefetch(size_t length, size_t offset, ReadOption option) noexcept
{
    if (unlikely(TEST_mDisableCache)) {
        return {FSEC_OK, 0};
    }
    return Read(NULL, length, offset, option);
}

Future<FSResult<size_t>> BlockFileAccessor::PrefetchAsync(size_t length, size_t offset, ReadOption option) noexcept
{
    if (unlikely(TEST_mDisableCache)) {
        return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_OK, 0});
    }
    return ReadAsync(NULL, length, offset, option);
}

FL_LAZY(FSResult<size_t>) BlockFileAccessor::PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept
{
    if (unlikely(TEST_mDisableCache)) {
        FL_CORETURN FSResult<size_t> {FSEC_OK, 0ul};
    }
    FL_CORETURN FL_COAWAIT ReadAsyncCoro(NULL, length, offset, option);
}

bool BlockFileAccessor::GetBlockMeta(size_t offset, BlockFileAccessor::FileBlockMeta& meta) noexcept
{
    if (offset >= _fileLength) {
        return false;
    }
    meta.blockOffset = GetBlockOffset(offset);
    meta.inBlockOffset = GetInBlockOffset(offset);
    meta.blockDataSize = min(_fileLength - meta.blockOffset, (size_t)_blockSize);
    return true;
}

Future<vector<FSResult<BlockHandle>>> BlockFileAccessor::GetBlockHandles(size_t blockInFileIdx, size_t blockCount,
                                                                         ReadOption option) noexcept
{
    vector<FSResult<BlockHandle>> result;
    if (blockCount == 0) {
        return makeReadyFuture(std::move(result));
    }
    result.resize(blockCount);
    int64_t timeout = -1;
    if (option.timeoutTerminator) {
        int64_t leftTime = option.timeoutTerminator->getLeftTime();
        if (leftTime > 0) {
            timeout = leftTime;
        } else {
            AUTIL_LOG(ERROR, "read block left time less than 0, timeout, file[%s]", _filePtr->GetFileName());
            for (auto& fsr : result) {
                fsr.ec = FSEC_OPERATIONTIMEOUT;
            }
            return makeReadyFuture(std::move(result));
        }
    }
    typedef vector<Block*> BlockArray;
    std::unique_ptr<BlockArray, std::function<void(BlockArray*)>> exceptionHandler(
        new BlockArray(blockCount, nullptr), [this](BlockArray* blocks) {
            for (Block* b : *blocks) {
                _blockAllocatorPtr->FreeBlock(b);
            }
            delete blocks;
        });
    std::vector<iovec> iov(blockCount);
    for (size_t i = 0; i < blockCount; ++i) {
        (*exceptionHandler)[i] = _blockAllocatorPtr->AllocBlock();
        (*exceptionHandler)[i]->id = util::blockid_t(_fileId, blockInFileIdx + i);
        iov[i].iov_base = (*exceptionHandler)[i]->data;
        iov[i].iov_len = _blockSize;
    }
    AddExecutorIfNotSet(&option, _executor);
    int64_t begin = autil::TimeUtility::currentTimeInMicroSeconds();
    auto future = _filePtr->PReadVAsync(iov.data(), blockCount, _fileBeginOffset + blockInFileIdx * _blockSize,
                                        option.advice, option.executor, timeout);
    if (!option.executor) {
        // synchronization mode, otherwise callback will execute in fslib(eg: pangu) callback thread pool,
        //  which is not recommanded
        future.wait();
    }
    return std::move(future).thenValue([this, begin, blockCount, option, exHandler = std::move(exceptionHandler),
                                        ioVector = std::move(iov),
                                        result = std::move(result)](FSResult<size_t>&& ret) mutable {
        if (!ret.OK()) {
            for (auto& fsr : result) {
                fsr.ec = ret.Code();
            }
            return std::move(result);
        }

        int64_t latency = TimeUtility::currentTimeInMicroSeconds() - begin;
        _blockCache->ReportReadLatency(latency, option.blockCounter);
        _blockCache->ReportReadSize(blockCount * _blockSize, option.blockCounter);

        _tagMetricReporter.ReportReadBlockCount(blockCount, option.trace);
        _tagMetricReporter.ReportReadSize(blockCount * _blockSize, option.trace);
        _tagMetricReporter.ReportReadLatency(latency, option.trace);

        auto readSize = ret.Value();
        if (option.blockCounter != nullptr && option.blockCounter->histCounter != nullptr) {
            option.blockCounter->histCounter->Report(blockCount, readSize, latency);
        }

        for (size_t i = 0; i < blockCount; ++i) {
            autil::CacheBase::Handle* cacheHandle = NULL;
            bool ret = _blockCache->Put((*exHandler)[i], &cacheHandle, _cachePriority);
            assert(ret);
            (void)ret;
            size_t blockDataSize = _blockSize;
            if (i == blockCount - 1 && readSize % _blockSize) {
                blockDataSize = readSize % _blockSize;
            }
            result[i].ec = FSEC_OK;
            result[i].Value().Reset(_blockCache, cacheHandle, (*exHandler)[i], blockDataSize);
        }
        BlockArray* array = exHandler.release();
        delete array;
        return std::move(result);
    });
}

FSResult<void> BlockFileAccessor::GetBlock(size_t offset, BlockHandle& handle, ReadOption* option) noexcept
{
    ReadOption localOption;
    localOption.advice = IO_ADVICE_LOW_LATENCY;
    if (option) {
        localOption = *option;
    }
    localOption.useInternalExecutor = false;

    ReadOption* optionPtr = &localOption;

    auto future = DoGetBlock(offset, *optionPtr);
    future.wait();
    auto& tryRet = future.result();
    assert(!tryRet.hasError());
    auto& ret = tryRet.value();
    RETURN_IF_FS_ERROR(ret.Code(), "DoGetBlock failed");
    handle = std::move(ret.Value());
    return {FSEC_OK};
}

Future<FSResult<BlockHandle>> BlockFileAccessor::GetBlockAsync(size_t offset, ReadOption option) noexcept
{
    return DoGetBlock(offset, option);
}

FL_LAZY(FSResult<BlockHandle>) BlockFileAccessor::GetBlockAsyncCoro(size_t offset, ReadOption option) noexcept
{
    AddExecutorIfNotSet(&option, FL_COAWAIT FL_CURRENT_EXECUTOR());
    FL_CORETURN FL_COAWAIT DoGetBlockCoro(offset, option);
}

size_t BlockFileAccessor::TryRead(void* buffer, size_t length, size_t offset) noexcept(false)
{
    std::vector<std::unique_ptr<BlockHandle>> handles = TryGetBlockHandles(offset, length);
    size_t leftLen = length;
    size_t beginOffset = offset;
    size_t toCopyLen = 0;
    uint8_t* dstCursor = (uint8_t*)buffer;
    for (size_t i = 0; i < handles.size(); i++) {
        if (handles[i] == nullptr) {
            return 0;
        }
        size_t inBlockOffset = GetInBlockOffset(beginOffset);
        toCopyLen = std::min(handles[i]->GetDataSize() - inBlockOffset, leftLen);
        memcpy(dstCursor, handles[i]->GetData() + inBlockOffset, toCopyLen);
        dstCursor += toCopyLen;
        beginOffset += toCopyLen;
        leftLen -= toCopyLen;
    }
    if (leftLen != 0) {
        return 0;
    }
    return length;
}

std::vector<std::unique_ptr<BlockHandle>> BlockFileAccessor::TryGetBlockHandles(size_t offset,
                                                                                size_t length) noexcept(false)
{
    std::vector<std::unique_ptr<BlockHandle>> handles;
    if (offset >= _fileLength || length == 0) {
        return handles;
    }
    size_t curBlockId = GetBlockIdx(offset);
    size_t endBlockId = GetBlockIdx(offset + length - 1);

    while (curBlockId <= endBlockId) {
        size_t blockOffset = curBlockId * _blockSize;
        blockid_t blockId(_fileId, curBlockId);
        CacheBase::Handle* handle = nullptr;
        Block* block = _blockCache->Get(blockId, &handle);
        if (NULL == block) {
            handles.push_back(nullptr);
        }
        size_t blockDataSize = min(_fileLength - blockOffset, (size_t)_blockSize);
        handles.push_back(std::make_unique<BlockHandle>(_blockCache, handle, block, blockDataSize));
        curBlockId++;
    }
    return handles;
}

Future<FSResult<BlockHandle>> BlockFileAccessor::DoGetBlock(size_t offset, ReadOption option) noexcept
{
    if (offset >= _fileLength) {
        AUTIL_LOG(ERROR, "offset[%lu] >= fileLength[%lu], file[%s]", offset, _fileLength, _filePtr->GetFileName());
        return makeReadyFuture<FSResult<BlockHandle>>({FSEC_BADARGS, BlockHandle()});
    }

    size_t blockOffset = GetBlockOffset(offset);
    blockid_t blockId(_fileId, blockOffset / _blockSize);
    return DoGetBlock(blockId, blockOffset, option)
#ifdef __clang__
        DIAGNOSTIC_PUSH DIAGNOSTIC_IGNORE("-Wunused-lambda-capture")
#endif
            .thenValue([blockId, this,
                        blockOffset](FSResult<std::pair<Block*, CacheBase::Handle*>>&& ret) -> FSResult<BlockHandle> {
#ifdef __clang__
                DIAGNOSTIC_POP
#endif
                util::BlockHandle handle;
                RETURN2_IF_FS_ERROR(ret.Code(), std::move(handle), "DoGetBlock failed");
                const auto& v = ret.Value();
                auto block = v.first;
                (void)blockId;
                auto cacheHandle = v.second;
                assert(block && block->id == blockId);
                size_t blockDataSize = min(_fileLength - blockOffset, (size_t)_blockSize);
                handle.Reset(_blockCache, cacheHandle, block, blockDataSize);
                return FSResult<BlockHandle> {FSEC_OK, std::move(handle)};
            });
}

FL_LAZY(FSResult<BlockHandle>) BlockFileAccessor::DoGetBlockCoro(size_t offset, ReadOption option) noexcept
{
    if (offset >= _fileLength) {
        assert(_filePtr);
        AUTIL_LOG(ERROR, "offset[%lu] >= fileLength[%lu], file[%s]", offset, _fileLength, _filePtr->GetFileName());
        FL_CORETURN FSResult<BlockHandle> {FSEC_BADARGS, BlockHandle()};
    }

    size_t blockOffset = GetBlockOffset(offset);
    blockid_t blockId(_fileId, blockOffset / _blockSize);
    auto [ec, v] = FL_COAWAIT DoGetBlockCoro(blockId, blockOffset, option);
    FL_CORETURN2_IF_FS_ERROR(ec, BlockHandle(), "DoGetBlockCoro failed, fileId[%lu], blockOffset[%lu], blockSize[%u]",
                             _fileId, blockOffset, _blockSize);
    auto block = v.first;
    (void)blockId;
    auto cacheHandle = v.second;
    assert(block && block->id == blockId);
    size_t blockDataSize = min(_fileLength - blockOffset, (size_t)_blockSize);
    util::BlockHandle handle;
    handle.Reset(_blockCache, cacheHandle, block, blockDataSize);
    FL_CORETURN FSResult<BlockHandle> {FSEC_OK, std::move(handle)};
}

Future<FSResult<std::pair<Block*, CacheBase::Handle*>>>
BlockFileAccessor::DoGetBlock(const blockid_t& blockID, uint64_t blockOffset, ReadOption option) noexcept
{
    assert(blockOffset % _blockSize == 0);
    CacheBase::Handle* handle = nullptr;
    Block* block = _blockCache->Get(blockID, &handle);

    if (NULL == block) {
        block = _blockAllocatorPtr->AllocBlock();
        block->id = blockID;
        _blockCache->ReportMiss(option.blockCounter);
        _tagMetricReporter.ReportMiss(option.trace);
        FreeBlockWhenException freeBlockWhenException(block, _blockAllocatorPtr.get());
        return ReadBlockFromFileToCache(block, blockOffset, option)
            .thenValue([block, guard = std::move(freeBlockWhenException)](FSResult<CacheBase::Handle*>&& ret) mutable
                       -> FSResult<std::pair<Block*, CacheBase::Handle*>> {
                RETURN2_IF_FS_ERROR(ret.Code(), std::make_pair(nullptr, nullptr), "ReadBlockFromFileToCache faield");
                guard.block = NULL; // normal return
                return FSResult<std::pair<Block*, CacheBase::Handle*>> {FSEC_OK, std::make_pair(block, ret.Value())};
            });
    } else {
        assert(block->id == blockID);
        _blockCache->ReportHit(option.blockCounter);
        _tagMetricReporter.ReportHit(option.trace);
        return makeReadyFuture(
            FSResult<std::pair<Block*, CacheBase::Handle*>> {FSEC_OK, std::make_pair(block, handle)});
    }
}

FL_LAZY(FSResult<std::pair<Block*, CacheBase::Handle*>>)
BlockFileAccessor::DoGetBlockCoro(const blockid_t& blockID, uint64_t blockOffset, ReadOption option) noexcept
{
    assert(blockOffset % _blockSize == 0);
    CacheBase::Handle* handle = nullptr;
    Block* block = FL_COAWAIT _blockCache->GetAsync(blockID, &handle);

    if (NULL == block) {
        block = _blockAllocatorPtr->AllocBlock();
        block->id = blockID;
        _blockCache->ReportMiss(option.blockCounter);
        _tagMetricReporter.ReportMiss(option.trace);
        FreeBlockWhenException freeBlockWhenException(block, _blockAllocatorPtr.get());
        try {
            FSResult<CacheBase::Handle*> newHandle =
                FL_COAWAIT ReadBlockFromFileToCache(block, blockOffset, option).toAwaiter();
            if (!newHandle.OK()) {
                FL_CORETURN FSResult<std::pair<Block*, CacheBase::Handle*>> {newHandle.Code(),
                                                                             std::make_pair(nullptr, nullptr)};
            }
            freeBlockWhenException.block = nullptr;
            FL_CORETURN FSResult<std::pair<Block*, CacheBase::Handle*>> {FSEC_OK,
                                                                         std::make_pair(block, newHandle.Value())};
        } catch (...) {
            FL_CORETURN FSResult<std::pair<Block*, CacheBase::Handle*>> {FSEC_ERROR, std::make_pair(nullptr, nullptr)};
        }
    } else {
        assert(block->id == blockID);
        _blockCache->ReportHit(option.blockCounter);
        _tagMetricReporter.ReportHit(option.trace);
        FL_CORETURN FSResult<std::pair<Block*, CacheBase::Handle*>> {FSEC_OK, std::make_pair(block, handle)};
    }
}

Future<FSResult<CacheBase::Handle*>> BlockFileAccessor::ReadBlockFromFileToCache(Block* block, uint64_t blockOffset,
                                                                                 ReadOption option) noexcept
{
    assert(blockOffset % _blockSize == 0);
    assert(_filePtr);

    AddExecutorIfNotSet(&option, _executor);
    int64_t begin = TimeUtility::currentTimeInMicroSeconds();

    auto future =
        _filePtr->PReadAsync(block->data, _blockSize, blockOffset + _fileBeginOffset, option.advice, option.executor);
    if (!option.executor) {
        // synchronization mode, wait
        future.wait();
    }
    return std::move(future).thenValue(
        [this, begin, block, option](FSResult<size_t>&& ret) -> FSResult<CacheBase::Handle*> {
            int64_t latency = TimeUtility::currentTimeInMicroSeconds() - begin;
            _blockCache->ReportReadLatency(latency, option.blockCounter);
            _blockCache->ReportReadSize(_blockSize, option.blockCounter);

            RETURN2_IF_FS_ERROR(ret.Code(), nullptr, "PReadAsync failed");
            auto readSize = ret.Value();

            _tagMetricReporter.ReportReadBlockCount(1, option.trace);
            _tagMetricReporter.ReportReadSize(_blockSize, option.trace);
            _tagMetricReporter.ReportReadLatency(latency, option.trace);

            if (option.blockCounter != nullptr && option.blockCounter->histCounter != nullptr) {
                option.blockCounter->histCounter->Report(1, readSize, latency);
            }

            CacheBase::Handle* handle = nullptr;
            bool putCacheRet = _blockCache->Put(block, &handle, _cachePriority);
            assert(putCacheRet);
            (void)putCacheRet;
            return FSResult<CacheBase::Handle*> {FSEC_OK, handle};
        });
}

void BlockFileAccessor::InitTagMetricReporter(FileSystemMetricsReporter* reporter, const string& path) noexcept
{
    if (_blockCache == nullptr) {
        return;
    }
    if (!reporter || !_blockCache->SupportReportTagMetrics()) {
        return;
    }
    map<string, string> tagMap;
    reporter->ExtractTagInfoFromPath(path, tagMap);
    _tagMetricReporter = _blockCache->DeclareTaggedMetricReporter(tagMap);
}

}} // namespace indexlib::file_system
