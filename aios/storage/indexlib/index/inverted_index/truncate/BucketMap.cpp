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
#include "indexlib/index/inverted_index/truncate/BucketMap.h"

#include <math.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BucketMap);
namespace {
static const std::string BUCKET_MAP_FILE_NAME_PREFIX = "bucket_map_";
}

bool BucketMap::Init(uint32_t size)
{
    assert(!_sortValues);
    if (0 == size) {
        return false;
    }

    _sortValues.reset(new uint32_t[size]);
    _totalCount = size;

    _bucketCount = GetBucketCount(size);
    _bucketSize = (_totalCount + _bucketCount - 1) / _bucketCount;
    assert(_bucketSize * _bucketCount >= size);
    return true;
}

uint32_t BucketMap::GetBucketCount(uint32_t size) const { return (uint32_t)sqrt(size); }

Status BucketMap::Store(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto writer = directory->CreateFileWriter(_name);
    auto st = writer->Write(&_totalCount, sizeof(_totalCount)).Status();
    RETURN_IF_STATUS_ERROR(st, "write total count failed.");
    st = writer->Write(_sortValues.get(), sizeof(_sortValues[0]) * _totalCount).Status();
    RETURN_IF_STATUS_ERROR(st, "write sort values failed.");
    st = writer->Close().Status();
    RETURN_IF_STATUS_ERROR(st, "close file writer failed.");
    return Status::OK();
}

Status BucketMap::Load(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto reader = directory->CreateFileReader(_name, indexlib::file_system::FSOpenType::FSOT_BUFFERED);
    auto st = reader->Open().Status();
    RETURN_IF_STATUS_ERROR(st, "open file failed.");
    return InnerLoad(reader);
}

Status BucketMap::InnerLoad(const std::shared_ptr<indexlib::file_system::FileReader>& reader)
{
    uint32_t expectSize = sizeof(_totalCount);
    auto [st, actualSize] = reader->Read(&_totalCount, expectSize).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "file read failed. expect size[%u]", expectSize);
    if (actualSize != expectSize) {
        AUTIL_LOG(ERROR, "file read failed, expect size[%u], actual size[%lu]", expectSize, actualSize);
        st = reader->Close().Status();
        RETURN_IF_STATUS_ERROR(st, "close file failed after read failed.");
        return Status::InternalError("file read failed.");
    }
    if (!Init(_totalCount)) {
        st = reader->Close().Status();
        RETURN_IF_STATUS_ERROR(st, "close file failed after init failed.");
        return Status::InternalError("file read failed.");
    }
    expectSize = sizeof(_sortValues[0]) * _totalCount;
    std::tie(st, actualSize) = reader->Read(_sortValues.get(), expectSize).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "read sort value failed. expect size[%u]", expectSize);
    if (actualSize != expectSize) {
        st = reader->Close().Status();
        RETURN_IF_STATUS_ERROR(st, "close file failed.");
        return Status::InternalError("file read failed.");
    }
    return reader->Close().Status();
}

int64_t BucketMap::EstimateMemoryUse() const { return _totalCount * sizeof(uint32_t); }

bool BucketMap::IsBucketMapFile(const std::string& fileName) { return fileName.find(BUCKET_MAP_FILE_NAME_PREFIX) != 0; }

std::string BucketMap::GetBucketMapFileName(const std::string& originalFileName)
{
    return BUCKET_MAP_FILE_NAME_PREFIX + originalFileName;
}
std::string BucketMap::GetOriginalFileName(const std::string& bucketMapFileName)
{
    return bucketMapFileName.substr(BUCKET_MAP_FILE_NAME_PREFIX.size());
}

BucketMap& BucketMap::operator=(BucketMap&& other)
{
    _totalCount = other._totalCount;
    _bucketCount = other._bucketCount;
    _bucketSize = other._bucketSize;
    _sortValues = std::move(other._sortValues);
    return *this;
}
} // namespace indexlib::index
