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
#include "indexlib/file_system/file/CompressHintDataReader.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressHintDataReader);

CompressHintDataReader::CompressHintDataReader()
    : _totalHintDataLen(0)
    , _hintBlockCount(0)
    , _trainHintBlockCount(0)
    , _totalHintMemUse(0)
    , _disableUseDataRef(false)
{
    _disableUseDataRef = DisableUseHintDataRef();
}

CompressHintDataReader::~CompressHintDataReader()
{
    for (size_t i = 0; i < _hintDataVec.size(); i++) {
        if (_hintDataVec[i] != nullptr) {
            DELETE_AND_SET_NULL(_hintDataVec[i]);
        }
    }
}

bool CompressHintDataReader::DisableUseHintDataRef()
{
    return autil::EnvUtil::getEnv("INDEXLIB_DISABLE_HINT_DATA_REF", false);
}

bool CompressHintDataReader::Load(BufferCompressor* compressor, const std::shared_ptr<FileReader>& fileReader,
                                  size_t beginOffset)
{
    if (!compressor) {
        AUTIL_LOG(ERROR, "compressor is null.");
        return false;
    }
    return LoadHintMeta(fileReader, beginOffset) && LoadHintData(compressor, fileReader, beginOffset);
}

bool CompressHintDataReader::LoadHintMeta(const std::shared_ptr<FileReader>& fileReader,
                                          size_t beginOffset) noexcept(false)
{
    if (fileReader->GetLogicLength() < beginOffset + sizeof(size_t) * 3) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "compress hint file [%s] is collapsed.",
                             fileReader->GetLogicalPath().c_str());
        return false;
    }
    size_t offset = fileReader->GetLogicLength() - sizeof(size_t) * 3;
    if (fileReader->Read(&_totalHintDataLen, sizeof(size_t), offset).GetOrThrow() != sizeof(size_t)) {
        INDEXLIB_FATAL_ERROR(FileIO, "read total hint data length fail from file [%s].",
                             fileReader->GetLogicalPath().c_str());
        return false;
    }
    if (fileReader->Read(&_hintBlockCount, sizeof(size_t), offset + sizeof(size_t)).GetOrThrow() != sizeof(size_t)) {
        INDEXLIB_FATAL_ERROR(FileIO, "read total hint block count fail from file [%s].",
                             fileReader->GetLogicalPath().c_str());
        return false;
    }
    if (fileReader->Read(&_trainHintBlockCount, sizeof(size_t), offset + sizeof(size_t) * 2).GetOrThrow() !=
        sizeof(size_t)) {
        INDEXLIB_FATAL_ERROR(FileIO, "read train hint block count fail from file [%s].",
                             fileReader->GetLogicalPath().c_str());
        return false;
    }

    size_t expectLength = beginOffset + _totalHintDataLen + _hintBlockCount * sizeof(size_t) + sizeof(size_t) * 3;
    if (expectLength != fileReader->GetLogicLength()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "compress hint file [%s] is collapsed, fileLength [%lu], "
                             "hint dataLength [%lu], hintBlockCount [%lu].",
                             fileReader->GetLogicalPath().c_str(), fileReader->GetLogicLength(), _totalHintDataLen,
                             _hintBlockCount);
        return false;
    }
    return true;
}

bool CompressHintDataReader::LoadHintData(util::BufferCompressor* compressor,
                                          const std::shared_ptr<FileReader>& fileReader,
                                          size_t beginOffset) noexcept(false)
{
    _hintDataVec.resize(_hintBlockCount, nullptr);
    std::vector<size_t> offsetVec;
    if (_hintBlockCount > 0) {
        offsetVec.resize(_hintBlockCount);
        size_t offsetDataLen = sizeof(size_t) * _hintBlockCount;
        if (fileReader->Read((void*)offsetVec.data(), offsetDataLen, beginOffset + _totalHintDataLen).GetOrThrow() !=
            offsetDataLen) {
            INDEXLIB_FATAL_ERROR(FileIO, "read offset data fail from hint file [%s]",
                                 fileReader->GetLogicalPath().c_str());
            return false;
        }

        if (_totalHintDataLen < offsetVec[_hintBlockCount - 1]) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                 "last hint block offset [%lu] is bigger than total Hint data length [%lu].",
                                 offsetVec[_hintBlockCount - 1], _totalHintDataLen);
            return false;
        }
    }

    const char* baseAddr = (const char*)fileReader->GetBaseAddress();
    size_t fileLength = fileReader->GetLogicLength();
    string hintData;
    for (size_t i = 0; i < _hintBlockCount; i++) {
        size_t offset = offsetVec[i];
        size_t len = (i + 1 == _hintBlockCount) ? _totalHintDataLen - offset : offsetVec[i + 1] - offset;
        if (baseAddr != nullptr && !_disableUseDataRef) {
            if (beginOffset + offset + len > fileLength) {
                INDEXLIB_FATAL_ERROR(FileIO,
                                     "read hint data fail from hint file [%s], offset [%lu], len [%lu], "
                                     "over file length [%lu]",
                                     fileReader->GetLogicalPath().c_str(), beginOffset + offset, len, fileLength);
            }
            autil::StringView hintStr(baseAddr + beginOffset + offset, len);
            _hintDataVec[i] = compressor->CreateHintData(hintStr, false);
            continue;
        }

        if (len > hintData.size()) {
            hintData.resize(len);
        }
        if (fileReader->Read((void*)hintData.data(), len, beginOffset + offset).GetOrThrow() != len) {
            INDEXLIB_FATAL_ERROR(FileIO, "read hint data fail from hint file [%s], offset [%lu], len [%lu]",
                                 fileReader->GetLogicalPath().c_str(), beginOffset + offset, len);
            return false;
        }
        autil::StringView hintStr =
            (len > 0) ? autil::StringView(hintData.c_str(), len) : autil::StringView::empty_instance();
        _hintDataVec[i] = compressor->CreateHintData(hintStr, true);
        _totalHintMemUse += len;
    }
    return true;
}

size_t CompressHintDataReader::GetMemoryUse() const
{
    return sizeof(*this) + _hintDataVec.size() * sizeof(util::CompressHintData*) + _totalHintMemUse;
}

}} // namespace indexlib::file_system
