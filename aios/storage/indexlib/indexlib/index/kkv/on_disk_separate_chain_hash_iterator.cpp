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
#include "indexlib/index/kkv/on_disk_separate_chain_hash_iterator.h"

#include "indexlib/index_define.h"
using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskSeparateChainHashIterator);

OnDiskSeparateChainHashIterator::OnDiskSeparateChainHashIterator() : mSize(0), mCursor(0), mKey(0) {}

OnDiskSeparateChainHashIterator::~OnDiskSeparateChainHashIterator() {}

void OnDiskSeparateChainHashIterator::Open(const KKVIndexConfigPtr& config, const SegmentData& segmentData)
{
    assert(config);
    DirectoryPtr dataDir = segmentData.GetIndexDirectory(config->GetIndexName(), true);
    mFileReader = dataDir->CreateFileReader(PREFIX_KEY_FILE_NAME,
                                            file_system::ReaderOption::CacheFirst(file_system::FSOT_BUFFERED));

    uint32_t bucketCount = 0;
    HashTable::DecodeMeta(mFileReader, mSize, bucketCount);
    mCursor = 0;
    if (mSize > 0) {
        ReadCurrentKeyValue();
    }
}

bool OnDiskSeparateChainHashIterator::IsValid() const { return mCursor < mSize; }

void OnDiskSeparateChainHashIterator::MoveToNext()
{
    ++mCursor;
    if (mCursor < mSize) {
        ReadCurrentKeyValue();
    }
}

void OnDiskSeparateChainHashIterator::SortByKey()
{
    IE_LOG(INFO, "sort will do nothing for on-disk separate chain hash table");
}

size_t OnDiskSeparateChainHashIterator::Size() const { return mSize; }

uint64_t OnDiskSeparateChainHashIterator::Key() const { return mKey; }

OnDiskPKeyOffset OnDiskSeparateChainHashIterator::Value() const { return mValue; }

void OnDiskSeparateChainHashIterator::ReadCurrentKeyValue()
{
    size_t cursor = mCursor * sizeof(HashNode);
    assert(mFileReader);
    HashNode node;
    mFileReader->Read(&node, sizeof(HashNode), cursor).GetOrThrow();
    mKey = node.key;
    mValue = node.value;
}
}} // namespace indexlib::index
