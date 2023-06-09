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

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVDiskIndexer : public IDiskIndexer
{
public:
    KKVDiskIndexer(int64_t timestamp, bool isRealtimeSegment)
        : _timestamp(autil::TimeUtility::us2sec(timestamp))
        , _isRealtimeSegment(isRealtimeSegment)
    {
    }
    ~KKVDiskIndexer() = default;

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    const std::shared_ptr<KKVBuiltSegmentReader<SKeyType>>& GetReader() const { return _reader; }
    const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& GetIndexConfig() const { return _indexConfig; }
    const std::shared_ptr<indexlib::file_system::IDirectory>& GetIndexDirectory() const { return _indexDirectory; }

private:
    uint32_t _timestamp = 0;
    bool _isRealtimeSegment = false;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDirectory;
    std::shared_ptr<KKVBuiltSegmentReader<SKeyType>> _reader;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVDiskIndexer, SKeyType);

template <typename SKeyType>
Status KKVDiskIndexer<SKeyType>::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::ConfigError("cast index config failed");
    }
    _indexDirectory = indexDirectory;
    auto reader = std::make_shared<KKVBuiltSegmentReader<SKeyType>>(_timestamp, _isRealtimeSegment);
    auto s = reader->Open(_indexConfig, _indexDirectory);
    if (!s.IsOK()) {
        return s;
    }
    _reader = std::move(reader);
    return Status::OK();
}

template <typename SKeyType>
size_t
KKVDiskIndexer<SKeyType>::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto kkvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    if (!kkvIndexConfig) {
        AUTIL_LOG(ERROR, "index: %s, type %s is not an kkv index", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return 0;
    }

    auto [status, kkvDir] = indexDirectory->GetDirectory(kkvIndexConfig->GetIndexName()).StatusWith();
    if (!status.IsOK() || !kkvDir) {
        AUTIL_LOG(ERROR, "directory for kkv index: %s does not exist", kkvIndexConfig->GetIndexName().c_str());
        return 0;
    }

    size_t pkeyMemory =
        kkvDir->EstimateFileMemoryUse(PREFIX_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG).GetOrThrow();
    size_t skeyMemory =
        kkvDir->EstimateFileMemoryUse(SUFFIX_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG).GetOrThrow();
    size_t valueMemory =
        kkvDir->EstimateFileMemoryUse(KKV_VALUE_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG).GetOrThrow();

    const auto& indexPreference = kkvIndexConfig->GetIndexPreference();
    size_t skeyCompressMapperMemory = 0;
    if (indexPreference.GetSkeyParam().EnableFileCompress()) {
        skeyCompressMapperMemory = indexlib::file_system::CompressFileAddressMapper::EstimateMemUsed(
            kkvDir, SUFFIX_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    }
    size_t valueCompressMapperMemory = 0;
    if (!indexPreference.IsValueInline() && indexPreference.GetValueParam().EnableFileCompress()) {
        valueCompressMapperMemory = indexlib::file_system::CompressFileAddressMapper::EstimateMemUsed(
            kkvDir, KKV_VALUE_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    } else {
        // TODO(qisa.cb) we don't know whether the value is inline now
        valueCompressMapperMemory = skeyCompressMapperMemory * 2;
    }
    return pkeyMemory + skeyMemory + valueMemory + skeyCompressMapperMemory + valueCompressMapperMemory;
}

template <typename SKeyType>
size_t KKVDiskIndexer<SKeyType>::EvaluateCurrentMemUsed()
{
    return _reader ? _reader->EvaluateCurrentMemUsed() : 0;
}

} // namespace indexlibv2::index
