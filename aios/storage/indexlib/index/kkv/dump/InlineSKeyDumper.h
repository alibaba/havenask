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

#include "autil/MultiValueFormatter.h"
#include "indexlib/index/kkv/common/ChunkWriter.h"
#include "indexlib/index/kkv/common/InlineOnDiskSKeyNode.h"
#include "indexlib/index/kkv/dump/SKeyDumperBase.h"

namespace indexlibv2::index {

template <typename SKeyType>
class InlineSKeyDumper final : public SKeyDumperBase
{
public:
    InlineSKeyDumper(bool storeTs, bool storeExpireTime, int32_t valueFixedLen)
        : SKeyDumperBase(storeTs, storeExpireTime)
        , _valueFixedLen(valueFixedLen)
        , _curMaxValueLen(valueFixedLen > 0 ? valueFixedLen : 0ul)
    {
    }
    ~InlineSKeyDumper() = default;

public:
    std::pair<Status, ChunkItemOffset> Dump(bool isDeletedPkey, bool isDeletedSkey, bool isLastSkey, SKeyType skey,
                                            uint32_t ts, uint32_t expireTime, autil::StringView value)
    {
        auto skeyNode = BuildSKeyNode<InlineOnDiskSKeyNode>(isDeletedPkey, isDeletedSkey, isLastSkey, skey,
                                                            ChunkItemOffset::Invalid());

        auto [status, skeyOffset] = DumpSKey(skeyNode, ts, expireTime);
        RETURN2_STATUS_DIRECTLY_IF_ERROR(status, ChunkItemOffset::Invalid());

        if (!isDeletedPkey && !isDeletedSkey) {
            DumpValue(value);
        }
        return {Status::OK(), skeyOffset};
    }

    size_t GetMaxValueLen() const { return _curMaxValueLen; }

private:
    void DumpValue(autil::StringView value)
    {
        assert(_chunkWriter);

        if (_valueFixedLen > 0) {
            assert((size_t)_valueFixedLen == value.size());
            _chunkWriter->AppendData(value);
            return;
        }

        char head[4];
        size_t headSize = autil::MultiValueFormatter::encodeCount(value.size(), head, 4);
        _chunkWriter->AppendData({head, headSize});
        _chunkWriter->AppendData(value);
        _curMaxValueLen = std::max(_curMaxValueLen, headSize + value.size());
    }

private:
    int32_t _valueFixedLen;
    size_t _curMaxValueLen;
};

} // namespace indexlibv2::index
