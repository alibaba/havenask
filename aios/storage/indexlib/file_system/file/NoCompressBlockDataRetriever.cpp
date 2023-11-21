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
#include "indexlib/file_system/file/NoCompressBlockDataRetriever.h"

#include "indexlib/file_system/file/BlockFileAccessor.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, NoCompressBlockDataRetriever);

NoCompressBlockDataRetriever::NoCompressBlockDataRetriever(const ReadOption& option, BlockFileAccessor* accessor)
    : BlockDataRetriever(accessor->GetBlockSize(), accessor->GetFileLength())
    , _prefetcher(accessor, option)
    , _accessor(accessor)
    , _readOption(option)

{
}

NoCompressBlockDataRetriever::~NoCompressBlockDataRetriever() { ReleaseBlocks(); }

FSResult<uint8_t*> NoCompressBlockDataRetriever::RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                                                                   size_t& blockDataLength) noexcept
{
    if (!GetBlockMeta(fileOffset, blockDataBeginOffset, blockDataLength)) {
        return {FSEC_OK, nullptr};
    }

    util::BlockHandle blockHandle;
    auto ret = _accessor->GetBlock(blockDataBeginOffset, blockHandle, &_readOption);
    RETURN2_IF_FS_ERROR(ret.Code(), nullptr, "GetBlock io exception, offset: [%lu]", blockDataBeginOffset);
    uint8_t* data = blockHandle.GetData();
    _handles.push_back(move(blockHandle));
    return {FSEC_OK, data};
}

future_lite::coro::Lazy<ErrorCode> NoCompressBlockDataRetriever::Prefetch(size_t fileOffset, size_t length) noexcept
{
    co_return co_await _prefetcher.Prefetch(fileOffset, length);
}

void NoCompressBlockDataRetriever::Reset() noexcept { ReleaseBlocks(); }

void NoCompressBlockDataRetriever::ReleaseBlocks() noexcept
{
    _handles.clear();
    _prefetcher.Reset();
}
}} // namespace indexlib::file_system
