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
#pragma once

#include <type_traits>

#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/ChunkWriter.h"
#include "indexlib/index/kkv/common/InlineOnDiskSKeyNode.h"
#include "indexlib/index/kkv/common/NormalOnDiskSKeyNode.h"

namespace indexlibv2::index {

class SKeyDumperBase
{
public:
    SKeyDumperBase(bool storeTs, bool storeExpireTime)
        : _storeTs(storeTs)
        , _storeExpireTime(storeExpireTime)
        , _maxSkeyCount(0)
        , _curSkeyCount(0)
        , _totalSkeyCount(0)
    {
    }
    virtual ~SKeyDumperBase() = default;

public:
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::file_system::WriterOption& writerOption)
    {
        // 不同于value chunk offset, skey chunk offset不对齐
        size_t skeyChunkOffsetAlignBit = 0;
        _chunkWriter = std::make_unique<ChunkWriter>(skeyChunkOffsetAlignBit);
        assert(_chunkWriter);
        return _chunkWriter->Open(directory, writerOption, SUFFIX_KEY_FILE_NAME);
    }
    Status Close()
    {
        assert(_chunkWriter);
        return _chunkWriter->Close();
    }
    size_t GetMaxSKeyCount() const { return _maxSkeyCount; }
    size_t GetTotalSKeyCount() const { return _totalSkeyCount; }

    template <typename SKeyType>
    static size_t GetTotalDumpSize(size_t totalSKeyCount, bool storeTs, bool storeExpireTime);

protected:
    template <template <typename> typename SKeyNode, typename SKeyType>
    SKeyNode<SKeyType> BuildSKeyNode(bool isDeletedPkey, bool isDeletedSkey, bool isLastSkey, SKeyType skey,
                                     ChunkItemOffset valueOffset);

    template <template <typename> typename SKeyNode, typename SKeyType>
    std::pair<Status, ChunkItemOffset> DumpSKey(const SKeyNode<SKeyType>& skeyNode, uint32_t ts, uint32_t expireTime);

protected:
    std::unique_ptr<indexlibv2::index::ChunkWriter> _chunkWriter;

private:
    bool _storeTs;
    bool _storeExpireTime;
    size_t _maxSkeyCount;
    size_t _curSkeyCount;
    size_t _totalSkeyCount;
};

template <template <typename> typename SKeyNode, typename SKeyType>
std::pair<Status, ChunkItemOffset> SKeyDumperBase::DumpSKey(const SKeyNode<SKeyType>& skeyNode, uint32_t ts,
                                                            uint32_t expireTime)
{
    assert(_chunkWriter);

    auto skeyOffset = _chunkWriter->InsertItem({reinterpret_cast<const char*>(&skeyNode), sizeof(skeyNode)});
    if (!skeyOffset.IsValidOffset()) {
        return {Status::IOError("insert skey node failed"), ChunkItemOffset::Invalid()};
    }
    if (_storeTs) {
        _chunkWriter->AppendData({reinterpret_cast<const char*>(&ts), sizeof(ts)});
    }
    if (_storeExpireTime) {
        _chunkWriter->AppendData({reinterpret_cast<const char*>(&expireTime), sizeof(expireTime)});
    }

    ++_totalSkeyCount;
    ++_curSkeyCount;
    if (skeyNode.IsLastNode()) {
        _maxSkeyCount = std::max(_curSkeyCount, _maxSkeyCount);
        _curSkeyCount = 0;
    }

    return {Status::OK(), skeyOffset};
}

template <template <typename> typename SKeyNode, typename SKeyType>
SKeyNode<SKeyType> SKeyDumperBase::BuildSKeyNode(bool isDeletedPkey, bool isDeletedSkey, bool isLastSkey, SKeyType skey,
                                                 ChunkItemOffset valueOffset)
{
    using SKeyNodeTyped = SKeyNode<SKeyType>;

    static_assert(std::is_trivially_copyable_v<SKeyNodeTyped>);
    if (isDeletedPkey) {
        return SKeyNodeTyped::PKeyDeletedNode(isLastSkey);
    } else if (isDeletedSkey) {
        return SKeyNodeTyped::SKeyDeletedNode(skey, isLastSkey);
    } else {
        if constexpr (std::is_same_v<SKeyNodeTyped, InlineOnDiskSKeyNode<SKeyType>>) {
            assert(valueOffset == ChunkItemOffset::Invalid());
            return SKeyNodeTyped::Of(skey, isLastSkey);
        } else {
            static_assert(std::is_same_v<SKeyNodeTyped, NormalOnDiskSKeyNode<SKeyType>>);
            return SKeyNodeTyped::Of(skey, valueOffset, isLastSkey);
        }
    }
}

template <typename SKeyType>
size_t SKeyDumperBase::GetTotalDumpSize(size_t totalSKeyCount, bool storeTs, bool storeExpireTime)
{
    size_t skeySize = sizeof(NormalOnDiskSKeyNode<SKeyType>);
    if (storeTs) {
        skeySize += sizeof(uint32_t);
    }
    if (storeExpireTime) {
        skeySize += sizeof(uint32_t);
    }
    size_t estimateSkeySize = totalSKeyCount * skeySize;
    size_t estimateChunkCount = (estimateSkeySize + KKV_CHUNK_SIZE_THRESHOLD - 1) / KKV_CHUNK_SIZE_THRESHOLD;
    return estimateSkeySize + estimateChunkCount * sizeof(ChunkMeta);
}

} // namespace indexlibv2::index
