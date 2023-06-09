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
#include "indexlib/index/kkv/merge/OnDiskKKVSegmentIterator.h"

#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/pkey_table/OnDiskPKeyHashIteratorCreator.h"
using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2::index {

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, OnDiskKKVSegmentIterator, SKeyType);

template <typename SKeyType>
OnDiskKKVSegmentIterator<SKeyType>::OnDiskKKVSegmentIterator(size_t segIdx)
    : _segmentIdx(segIdx)
    , _storeTs(true)
    , _keepSortSequence(false)
    , _defaultTs(0)
{
}

template <typename SKeyType>
OnDiskKKVSegmentIterator<SKeyType>::~OnDiskKKVSegmentIterator()
{
}

template <typename SKeyType>
void OnDiskKKVSegmentIterator<SKeyType>::Init(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig,
                                              const std::shared_ptr<framework::Segment>& segment)
{
    assert(kkvIndexConfig);
    _kkvIndexConfig = kkvIndexConfig;
    auto segmentDirectory = segment->GetSegmentDirectory()->GetIDirectory();
    assert(segmentDirectory);
    auto indexDirectory = segmentDirectory->GetDirectory(KKV_INDEX_PATH).GetOrThrow();
    auto result = indexDirectory->GetDirectory(kkvIndexConfig->GetIndexName());
    if (!result.OK()) {
        auto status = Status::Corruption("index dir [%s] does not exist in [%s]",
                                         kkvIndexConfig->GetIndexName().c_str(), indexDirectory->DebugString().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        // TODO(xinfei.sxf) add status
        return;
    }
    auto kkvDir = result.Value();

    _pkeyTableIter.reset(OnDiskPKeyHashIteratorCreator::CreateOnDiskPKeyIterator(kkvIndexConfig, kkvDir));

    KKVIndexFormat indexFormat;
    auto status = indexFormat.Load(kkvDir);
    if (!status.IsOK()) {
        INDEXLIB_THROW(indexlib::util::IndexCollapsedException, "failed to load indexFormat from dir:[%s]",
                       kkvDir->GetLogicalPath().c_str());
    }

    const auto& kkvIndexPreference = kkvIndexConfig->GetIndexPreference();
    const auto& skeyParam = kkvIndexPreference.GetSkeyParam();
    const auto& valueParam = kkvIndexPreference.GetValueParam();

    ReaderOption readOption = ReaderOption::CacheFirst(FSOT_BUFFERED);
    readOption.supportCompress = skeyParam.EnableFileCompress();
    _skeyReader = kkvDir->CreateFileReader(SUFFIX_KEY_FILE_NAME, readOption).GetOrThrow();
    if (!indexFormat.ValueInline()) {
        readOption.supportCompress = valueParam.EnableFileCompress();
        _valueReader = kkvDir->CreateFileReader(KKV_VALUE_FILE_NAME, readOption).GetOrThrow();
    }

    _storeTs = indexFormat.StoreTs();
    _keepSortSequence = indexFormat.KeepSortSequence();
    _defaultTs = autil::TimeUtility::us2sec(segment->GetSegmentInfo()->timestamp);

    bool valueInline = _valueReader == nullptr ? true : false;
    _iteratorFactory = std::make_unique<KKVBuiltSegmentIteratorFactory<SKeyType>>(
        _kkvIndexConfig.get(), false, _storeTs, _defaultTs, _keepSortSequence, valueInline, _skeyReader.get(),
        _valueReader.get());
}

template <typename SKeyType>
void OnDiskKKVSegmentIterator<SKeyType>::ResetBufferParam(size_t readerBufferSize, bool asyncRead)
{
    ResetBufferParam(_skeyReader, readerBufferSize, asyncRead);
    if (_valueReader) {
        ResetBufferParam(_valueReader, readerBufferSize, asyncRead);
    }
}

template <typename SKeyType>
bool OnDiskKKVSegmentIterator<SKeyType>::IsValid() const
{
    assert(_pkeyTableIter);
    return _pkeyTableIter->IsValid();
}

template <typename SKeyType>
void OnDiskKKVSegmentIterator<SKeyType>::MoveToNext() const
{
    assert(_pkeyTableIter);
    _pkeyTableIter->MoveToNext();
}

template <typename SKeyType>
uint64_t OnDiskKKVSegmentIterator<SKeyType>::GetPrefixKey() const
{
    assert(IsValid());
    uint64_t key;
    OnDiskPKeyOffset offset;
    _pkeyTableIter->Get(key, offset);
    return key;
}

template <typename SKeyType>
typename OnDiskKKVSegmentIterator<SKeyType>::SinglePKeyIterator*
OnDiskKKVSegmentIterator<SKeyType>::GetIterator(Pool* pool, const std::shared_ptr<config::KKVIndexConfig>& indexConfig)
{
    assert(pool);
    uint64_t key;
    OnDiskPKeyOffset offset;
    _pkeyTableIter->Get(key, offset);

    // TODO(xinfei.sxf) retry when io exception?
    auto [status, iterator] = future_lite::interface::syncAwait(_iteratorFactory->Create(offset, pool, nullptr));
    return iterator;
}

template <typename SKeyType>
void OnDiskKKVSegmentIterator<SKeyType>::ResetBufferParam(const FileReaderPtr& reader, size_t readerBufferSize,
                                                          bool asyncRead)
{
    assert(reader);
    FileReaderPtr dataReader = reader;
    CompressFileReaderPtr compressReader = std::dynamic_pointer_cast<CompressFileReader>(dataReader);
    if (compressReader) {
        dataReader = compressReader->GetDataFileReader();
    }

    BufferedFileReaderPtr bufferReader = std::dynamic_pointer_cast<BufferedFileReader>(dataReader);
    if (bufferReader) {
        bufferReader->ResetBufferParam(readerBufferSize, asyncRead);
    }
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(OnDiskKKVSegmentIterator);

} // namespace indexlibv2::index
