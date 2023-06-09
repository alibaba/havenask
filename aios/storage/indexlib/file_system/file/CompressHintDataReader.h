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

#include <memory>

#include "autil/Log.h"

namespace indexlib::util {
class BufferCompressor;
class CompressHintData;
} // namespace indexlib::util

namespace indexlib { namespace file_system {
class FileReader;
class CompressHintData;

class CompressHintDataReader
{
public:
    CompressHintDataReader();
    ~CompressHintDataReader();

    CompressHintDataReader(const CompressHintDataReader&) = delete;
    CompressHintDataReader& operator=(const CompressHintDataReader&) = delete;
    CompressHintDataReader(CompressHintDataReader&&) = delete;
    CompressHintDataReader& operator=(CompressHintDataReader&&) = delete;

public:
    bool Load(util::BufferCompressor* compressor, const std::shared_ptr<FileReader>& fileReader, size_t beginOffset);
    size_t GetMemoryUse() const;

    util::CompressHintData* GetHintData(size_t blockIdx) const
    {
        if (_trainHintBlockCount == 0) {
            return nullptr;
        }
        size_t hintBlockIdx = blockIdx / _trainHintBlockCount;
        return (hintBlockIdx < _hintBlockCount) ? _hintDataVec[hintBlockIdx] : nullptr;
    }

    static bool DisableUseHintDataRef();

private:
    bool LoadHintMeta(const std::shared_ptr<FileReader>& fileReader, size_t beginOffset) noexcept(false);
    bool LoadHintData(util::BufferCompressor* compressor, const std::shared_ptr<FileReader>& fileReader,
                      size_t beginOffset) noexcept(false);

private:
    std::vector<util::CompressHintData*> _hintDataVec;
    size_t _totalHintDataLen;
    size_t _hintBlockCount;
    size_t _trainHintBlockCount;
    size_t _totalHintMemUse;
    bool _disableUseDataRef;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressHintDataReader> CompressHintDataReaderPtr;

}} // namespace indexlib::file_system
