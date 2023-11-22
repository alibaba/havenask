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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib::index::legacy {

class BucketMap
{
public:
    static const std::string BUKCET_MAP_FILE_NAME_PREFIX;

public:
    BucketMap();
    ~BucketMap();

public:
    bool Init(uint32_t size);

    void SetSortValue(docid_t docId, uint32_t sortValue);
    uint32_t GetSortValue(docid_t docId);

    uint32_t GetBucketValue(docid_t docId);

    uint32_t GetBucketCount() const { return mBucketCount; }
    uint32_t GetBucketSize() const { return mBucketSize; }
    uint32_t GetSize() const { return mSize; }

    void Store(const file_system::DirectoryPtr& directory, const std::string& fileName) const;
    bool Load(const file_system::DirectoryPtr& directory, const std::string& fileName);

    int64_t EstimateMemoryUse() const;

private:
    uint32_t GetBucketCount(uint32_t size);
    bool InnerLoad(file_system::FileReaderPtr& reader);

private:
    uint32_t* mSortValues;
    uint32_t mSize;
    uint32_t mBucketCount;
    uint32_t mBucketSize;

private:
    IE_LOG_DECLARE();
};

inline void BucketMap::SetSortValue(docid_t docId, uint32_t sortValue)
{
    assert(mSortValues != NULL);
    assert((uint32_t)docId < mSize);
    assert(sortValue < mSize);

    mSortValues[docId] = sortValue;
}

inline uint32_t BucketMap::GetSortValue(docid_t docId)
{
    assert((uint32_t)docId < mSize);
    assert(mSortValues != NULL);
    return mSortValues[docId];
}

inline uint32_t BucketMap::GetBucketValue(docid_t docId)
{
    assert((uint32_t)docId < mSize);
    return mSortValues[docId] / mBucketSize;
}

DEFINE_SHARED_PTR(BucketMap);
typedef std::map<std::string, BucketMapPtr> BucketMaps;
} // namespace indexlib::index::legacy
