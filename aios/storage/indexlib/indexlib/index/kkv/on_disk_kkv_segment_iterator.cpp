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
#include "indexlib/index/kkv/on_disk_kkv_segment_iterator.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/index/kkv/kkv_built_segment_doc_iterator.h"
#include "indexlib/index/kkv/kkv_index_format.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskKKVSegmentIterator);

OnDiskKKVSegmentIterator::OnDiskKKVSegmentIterator(size_t segIdx)
    : mSegmentIdx(segIdx)
    , mStoreTs(true)
    , mKeepSortSequence(false)
    , mDefaultTs(0)
{
}

OnDiskKKVSegmentIterator::~OnDiskKKVSegmentIterator() {}

void OnDiskKKVSegmentIterator::Init(const KKVIndexConfigPtr& kkvIndexConfig, const SegmentData& segmentData)
{
    assert(kkvIndexConfig);
    mKKVIndexConfig = kkvIndexConfig;
    mPKeyTable.reset(OnDiskPKeyHashIteratorCreator::CreateOnDiskPKeyIterator(kkvIndexConfig, segmentData));
    DirectoryPtr dataDir = segmentData.GetIndexDirectory(kkvIndexConfig->GetIndexName(), true);
    KKVIndexFormat indexFormat;
    indexFormat.Load(dataDir);

    const KKVIndexPreference& kkvIndexPreference = kkvIndexConfig->GetIndexPreference();
    const KKVIndexPreference::SuffixKeyParam& skeyParam = kkvIndexPreference.GetSkeyParam();
    const KKVIndexPreference::ValueParam& valueParam = kkvIndexPreference.GetValueParam();

    ReaderOption readOption = ReaderOption::CacheFirst(FSOT_BUFFERED);
    readOption.supportCompress = skeyParam.EnableFileCompress();
    mSkeyReader = dataDir->CreateFileReader(SUFFIX_KEY_FILE_NAME, readOption);
    if (!indexFormat.ValueInline()) {
        readOption.supportCompress = valueParam.EnableFileCompress();
        mValueReader = dataDir->CreateFileReader(KKV_VALUE_FILE_NAME, readOption);
    }

    mSkeyDictType = GetSKeyDictKeyType(kkvIndexConfig->GetSuffixFieldConfig()->GetFieldType());

    mStoreTs = indexFormat.StoreTs();
    mKeepSortSequence = indexFormat.KeepSortSequence();
    mDefaultTs = MicrosecondToSecond(segmentData.GetSegmentInfo()->timestamp);
}

void OnDiskKKVSegmentIterator::ResetBufferParam(size_t readerBufferSize, bool asyncRead)
{
    ResetBufferParam(mSkeyReader, readerBufferSize, asyncRead);
    if (mValueReader) {
        ResetBufferParam(mValueReader, readerBufferSize, asyncRead);
    }
}

bool OnDiskKKVSegmentIterator::IsValid() const
{
    assert(mPKeyTable);
    return mPKeyTable->IsValid();
}

void OnDiskKKVSegmentIterator::MoveToNext() const
{
    assert(mPKeyTable);
    mPKeyTable->MoveToNext();
}

uint64_t OnDiskKKVSegmentIterator::GetPrefixKey(regionid_t& regionId) const
{
    assert(IsValid());
    uint64_t key;
    OnDiskPKeyOffset offset;
    mPKeyTable->Get(key, offset);
    regionId = offset.regionId;
    return key;
}

OnDiskKKVSegmentIterator::SinglePKeyIterator*
OnDiskKKVSegmentIterator::GetIterator(Pool* pool, const KKVIndexConfigPtr& regionKkvConfig)
{
    assert(pool);
    uint64_t key;
    OnDiskPKeyOffset offset;
    mPKeyTable->Get(key, offset);

    switch (mSkeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        typedef FieldTypeTraits<type>::AttrItemType SKeyType;                                                          \
        KKVBuiltSegmentDocIterator<SKeyType>* iter =                                                                   \
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, KKVBuiltSegmentDocIterator<SKeyType>, pool, false);                     \
        future_lite::interface::syncAwait(iter->Init(mSkeyReader, mValueReader, regionKkvConfig.get(), offset,         \
                                                     mDefaultTs, mStoreTs, mKeepSortSequence));                        \
        return iter;                                                                                                   \
    }
        NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        assert(false);
    }
    }
    return NULL;
}

void OnDiskKKVSegmentIterator::ResetBufferParam(const FileReaderPtr& reader, size_t readerBufferSize, bool asyncRead)
{
    assert(reader);
    FileReaderPtr dataReader = reader;
    CompressFileReaderPtr compressReader = DYNAMIC_POINTER_CAST(CompressFileReader, dataReader);
    if (compressReader) {
        dataReader = compressReader->GetDataFileReader();
    }

    BufferedFileReaderPtr bufferReader = DYNAMIC_POINTER_CAST(BufferedFileReader, dataReader);
    if (bufferReader) {
        bufferReader->ResetBufferParam(readerBufferSize, asyncRead);
    }
}
}} // namespace indexlib::index
