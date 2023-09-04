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
#include "indexlib/index/kkv/Types.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorFactory.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTableIterator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuiltSegmentReader
{
private:
    using PKeyTable = PrefixKeyTableBase<OnDiskPKeyOffset>;

public:
    KKVBuiltSegmentReader(uint32_t timestamp, bool isRealtimeSegment)
        : _timestamp(timestamp)
        , _isRealtimeSegment(isRealtimeSegment)
    {
    }
    ~KKVBuiltSegmentReader() {}

public:
    Status Open(const std::shared_ptr<config::KKVIndexConfig>& config,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);

    // TODO(xinfei.sxf) use Result instead of pair
    FL_LAZY(std::pair<Status, KKVBuiltSegmentIteratorBase<SKeyType>*>)
    Lookup(PKeyType pkey, autil::mem_pool::Pool* sessionPool, KVMetricsCollector* metricsCollector = nullptr) const
    {
        OnDiskPKeyOffset firstSkeyOffset;
        // TODO(xinfei.sxf) FindForRead api return status
        try {
            if (!FL_COAWAIT((PKeyTable*)_pkeyTable.get())->FindForRead(pkey, firstSkeyOffset, metricsCollector)) {
                FL_CORETURN std::make_pair(Status::OK(), nullptr);
            }
        } catch (const std::exception& e) {
            FL_CORETURN std::make_pair(Status::IOError(), nullptr);
        }
        FL_CORETURN FL_COAWAIT _iteratorFactory->Create(firstSkeyOffset, sessionPool, metricsCollector);
    }

    size_t EvaluateCurrentMemUsed();
    bool IsSkeyInMemory() { return _skeyInMemory; }
    bool IsValueInMemory() { return _valueInMemory; }
    bool IsValueInline() { return _valueInline; }
    bool IsRealtimeSegment() const { return _isRealtimeSegment; }
    uint32_t GetTimestampInSecond() const { return _timestamp; }

private:
    std::shared_ptr<indexlib::file_system::FileReader>
    CreateValueReader(const std::shared_ptr<indexlib::file_system::IDirectory>& dir);

private:
    std::shared_ptr<config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<PKeyTable> _pkeyTable;
    std::shared_ptr<indexlib::file_system::FileReader> _skeyReader;
    std::shared_ptr<indexlib::file_system::FileReader> _valueReader;
    std::unique_ptr<KKVBuiltSegmentIteratorFactory<SKeyType>> _iteratorFactory;
    uint32_t _timestamp = 0;
    bool _isRealtimeSegment = false;
    bool _storeTs = false;
    bool _valueInline = false;
    bool _keepSortSeq = false;
    bool _skeyInMemory = false;
    bool _valueInMemory = false;

private:
    friend class KKVReaderTest;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVBuiltSegmentReader, SKeyType);

template <typename SKeyType>
inline Status
KKVBuiltSegmentReader<SKeyType>::Open(const std::shared_ptr<config::KKVIndexConfig>& indexConfig,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _indexConfig = indexConfig;
    auto result = indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    RETURN_IF_STATUS_ERROR(result.Status(), "index dir [%s] does not exist in [%s]",
                           _indexConfig->GetIndexName().c_str(), indexDirectory->DebugString().c_str());

    auto kkvDir = result.Value();
    const auto& indexPreference = _indexConfig->GetIndexPreference();
    _pkeyTable.reset((PKeyTable*)PrefixKeyTableCreator<OnDiskPKeyOffset>::Create(kkvDir, indexPreference,
                                                                                 PKeyTableOpenType::READ, 0, 0));

    const auto& skeyParam = indexPreference.GetSkeyParam();
    indexlib::file_system::ReaderOption readOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    readOption.supportCompress = skeyParam.EnableFileCompress();
    _skeyReader = kkvDir->CreateFileReader(SUFFIX_KEY_FILE_NAME, readOption).GetOrThrow();
    _skeyInMemory = _skeyReader->GetBaseAddress() != nullptr;

    KKVIndexFormat indexFormat;
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexFormat.Load(kkvDir));
    _storeTs = indexFormat.StoreTs();
    _keepSortSeq = indexFormat.KeepSortSequence();
    _valueInline = indexFormat.ValueInline();
    if (!_valueInline) {
        _valueReader = CreateValueReader(kkvDir);
        _valueInMemory = _valueReader->GetBaseAddress() != nullptr;
    }

    _iteratorFactory = std::make_unique<KKVBuiltSegmentIteratorFactory<SKeyType>>(
        _indexConfig.get(), /*isOnline*/ true, _storeTs, _timestamp, _keepSortSeq, _valueInline, _skeyReader.get(),
        _valueReader.get());
    return Status::OK();
}

template <typename SKeyType>
inline size_t KKVBuiltSegmentReader<SKeyType>::EvaluateCurrentMemUsed()
{
    size_t pkeyMemUse = _pkeyTable->GetTotalMemoryUse();
    size_t skeyMemUse = _skeyReader->EvaluateCurrentMemUsed();
    size_t valueMemUse = _valueReader ? _valueReader->EvaluateCurrentMemUsed() : 0;
    return pkeyMemUse + skeyMemUse + valueMemUse;
}

template <typename SKeyType>
inline std::shared_ptr<indexlib::file_system::FileReader> KKVBuiltSegmentReader<SKeyType>::CreateValueReader(
    const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    const auto& indexPreference = _indexConfig->GetIndexPreference();
    const auto& valueParam = indexPreference.GetValueParam();
    readerOption.supportCompress = valueParam.EnableFileCompress();
    return indexDirectory->CreateFileReader(KKV_VALUE_FILE_NAME, readerOption).GetOrThrow();
}

} // namespace indexlibv2::index
