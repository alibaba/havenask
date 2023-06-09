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
#include "indexlib/index/merger_util/truncate/bucket_map.h"

#include <math.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, BucketMap);

const std::string BucketMap::BUKCET_MAP_FILE_NAME_PREFIX = "bucket_map_";

BucketMap::BucketMap()
{
    mSortValues = NULL;
    mSize = 0;
    mBucketCount = 0;
    mBucketSize = 0;
}

BucketMap::~BucketMap()
{
    if (mSortValues) {
        delete[] mSortValues;
        mSortValues = NULL;
    }
}

bool BucketMap::Init(uint32_t size)
{
    assert(!mSortValues);
    if (0 == size) {
        return false;
    }
    mSortValues = new uint32_t[size];
    mSize = size;

    mBucketCount = GetBucketCount(size);
    mBucketSize = (mSize + mBucketCount - 1) / mBucketCount;
    assert(mBucketSize * mBucketCount >= size);
    return true;
}

uint32_t BucketMap::GetBucketCount(uint32_t size) { return (uint32_t)sqrt(size); }

void BucketMap::Store(const DirectoryPtr& directory, const string& fileName) const
{
    FileWriterPtr writer = directory->CreateFileWriter(fileName);
    writer->Write(&mSize, sizeof(mSize)).GetOrThrow();
    writer->Write(mSortValues, sizeof(mSortValues[0]) * mSize).GetOrThrow();
    writer->Close().GetOrThrow();
}

bool BucketMap::Load(const DirectoryPtr& directory, const string& fileName)
{
    FileReaderPtr reader = directory->CreateFileReader(fileName, FSOpenType::FSOT_BUFFERED);
    THROW_IF_FS_ERROR(reader->Open(), "file reader open failed");
    return InnerLoad(reader);
}

bool BucketMap::InnerLoad(file_system::FileReaderPtr& reader)
{
    uint32_t expectSize = sizeof(mSize);
    if (reader->Read(&mSize, expectSize).GetOrThrow() != expectSize) {
        reader->Close().GetOrThrow();
        return false;
    }
    if (!Init(mSize)) {
        reader->Close().GetOrThrow();
        return false;
    }
    expectSize = sizeof(mSortValues[0]) * mSize;
    if (reader->Read(mSortValues, expectSize).GetOrThrow() != expectSize) {
        reader->Close().GetOrThrow();
        return false;
    }
    reader->Close().GetOrThrow();
    return true;
}

int64_t BucketMap::EstimateMemoryUse() const { return mSize * sizeof(uint32_t); }
} // namespace indexlib::index::legacy
