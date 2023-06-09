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

#include <algorithm>
#include <memory>
#include <stddef.h>
#include <vector>

#include "autil/Log.h"

namespace indexlib { namespace file_system {

class CompressFileMeta
{
public:
    // first: uncompress endPos, second: compress endPos
    struct CompressBlockMeta {
        CompressBlockMeta(size_t _uncompressEndPos, size_t _compressEndPos)
            : uncompressEndPos(_uncompressEndPos)
            , compressEndPos(_compressEndPos)
        {
        }

        size_t uncompressEndPos;
        size_t compressEndPos;
    };

public:
    CompressFileMeta() {}
    ~CompressFileMeta() {}

public:
    bool Init(size_t blockCount, const char* buffer)
    {
        CompressBlockMeta* blockAddr = (CompressBlockMeta*)buffer;
        for (size_t i = 0; i < blockCount; i++) {
            if (!AddOneBlock(blockAddr[i])) {
                return false;
            }
        }
        return true;
    }

public:
    bool AddOneBlock(const CompressBlockMeta& blockMeta)
    {
        return AddOneBlock(blockMeta.uncompressEndPos, blockMeta.compressEndPos);
    }

    bool AddOneBlock(size_t uncompressEndPos, size_t compressEndPos)
    {
        if (_blockMetas.empty()) {
            _blockMetas.push_back(CompressBlockMeta(uncompressEndPos, compressEndPos));
            return true;
        }

        if (_blockMetas.back().uncompressEndPos >= uncompressEndPos ||
            _blockMetas.back().compressEndPos >= compressEndPos) {
            return false;
        }
        _blockMetas.push_back(CompressBlockMeta(uncompressEndPos, compressEndPos));
        return true;
    }

    size_t GetBlockCount() const { return _blockMetas.size(); }

    char* Data() const { return (char*)_blockMetas.data(); }

    size_t Size() const { return _blockMetas.size() * sizeof(CompressBlockMeta); }

    const std::vector<CompressBlockMeta>& GetCompressBlockMetas() const { return _blockMetas; }

    size_t GetMaxCompressBlockSize() const
    {
        size_t maxBlockSize = 0;
        size_t preEndPos = 0;
        for (size_t i = 0; i < _blockMetas.size(); ++i) {
            size_t curBlockSize = _blockMetas[i].compressEndPos - preEndPos;
            maxBlockSize = std::max(maxBlockSize, curBlockSize);
            preEndPos = _blockMetas[i].compressEndPos;
        }
        return maxBlockSize;
    }

    size_t GetUnCompressFileLength() const { return _blockMetas.empty() ? 0 : _blockMetas.back().uncompressEndPos; }

public:
    static size_t GetMetaLength(size_t blockCount) { return blockCount * sizeof(CompressBlockMeta); }

private:
    std::vector<CompressBlockMeta> _blockMetas;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressFileMeta> CompressFileMetaPtr;
}} // namespace indexlib::file_system
