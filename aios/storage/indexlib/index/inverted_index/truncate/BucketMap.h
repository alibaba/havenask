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
#include <assert.h>
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlib::file_system {
class Directory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlib::index {

class BucketMap : public indexlibv2::framework::IndexTaskResource
{
public:
    BucketMap(std::string name, indexlibv2::framework::IndexTaskResourceType type)
        : indexlibv2::framework::IndexTaskResource(std::move(name), type)
    {
    }
    ~BucketMap() = default;

    BucketMap& operator=(BucketMap&& other);

public:
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& directory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& directory) override;

public:
    bool Init(uint32_t size);
    void SetSortValue(docid_t docId, uint32_t sortValue);
    uint32_t GetSortValue(docid_t docId) const;
    uint32_t GetBucketValue(docid_t docId) const;
    uint32_t GetBucketCount() const { return _bucketCount; }
    uint32_t GetBucketSize() const { return _bucketSize; }
    uint32_t GetSize() const { return _totalCount; }
    int64_t EstimateMemoryUse() const;

    static bool IsBucketMapFile(const std::string& fileName);
    // return bucket_map_${orignalFileName}
    static std::string GetBucketMapFileName(const std::string& originalFileName);
    // return orignalFileName
    static std::string GetOriginalFileName(const std::string& bucketMapFileName);
    static std::string GetBucketMapType() { return "BUCKET_MAP"; }

private:
    uint32_t GetBucketCount(uint32_t size) const;
    Status InnerLoad(const std::shared_ptr<indexlib::file_system::FileReader>& reader);

private:
    std::unique_ptr<uint32_t[]> _sortValues;
    uint32_t _totalCount = 0;
    uint32_t _bucketCount = 0;
    uint32_t _bucketSize = 0;

private:
    AUTIL_LOG_DECLARE();
};

inline void BucketMap::SetSortValue(docid_t docId, uint32_t sortValue)
{
    assert(_sortValues != nullptr);
    assert((uint32_t)docId < _totalCount);
    assert(sortValue < _totalCount);

    _sortValues[docId] = sortValue;
}

inline uint32_t BucketMap::GetSortValue(docid_t docId) const
{
    assert((uint32_t)docId < _totalCount);
    assert(_sortValues != nullptr);
    return _sortValues[docId];
}

inline uint32_t BucketMap::GetBucketValue(docid_t docId) const
{
    assert((uint32_t)docId < _totalCount);
    return _sortValues[docId] / _bucketSize;
}

using BucketMaps = std::map<std::string, std::shared_ptr<BucketMap>>;
} // namespace indexlib::index
