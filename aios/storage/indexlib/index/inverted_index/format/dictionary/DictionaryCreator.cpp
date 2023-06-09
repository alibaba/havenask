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
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskHashDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryTypedFactory.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/HashDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/HashDictionaryWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DictionaryCreator);

DictionaryReader*
DictionaryCreator::CreateReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config)
{
    if (config->IsHashTypedDictionary()) {
        return DictionaryTypedFactory<DictionaryReader, HashDictionaryReaderTyped>::GetInstance()->Create(
            config->GetInvertedIndexType());
    }
    auto reader = DictionaryTypedFactory<DictionaryReader, TieredDictionaryReaderTyped>::GetInstance()->Create(
        config->GetInvertedIndexType());
    uint32_t multipleNum;
    uint32_t hashFuncNum;
    if (config->GetBloomFilterParamForDictionary(multipleNum, hashFuncNum)) {
        reader->EnableBloomFilter(multipleNum, hashFuncNum);
    }
    return reader;
}

DictionaryWriter*
DictionaryCreator::CreateWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStatistics,
                                autil::mem_pool::PoolBase* pool)
{
    DictionaryWriter* writer = nullptr;
    if (config->IsHashTypedDictionary()) {
        writer = DictionaryTypedFactory<DictionaryWriter, HashDictionaryWriter>::GetInstance()->Create(
            config->GetInvertedIndexType(), pool);
        if (config->GetFileCompressConfigV2()) {
            AUTIL_LOG(WARN, "index [%s] with hash type dictionary will ignore file compress.",
                      config->GetIndexName().c_str());
        }
    } else {
        writer = DictionaryTypedFactory<DictionaryWriter, TieredDictionaryWriter>::GetInstance()->Create(
            config->GetInvertedIndexType(), pool);
        file_system::WriterOption option;
        indexlibv2::index::FileCompressParamHelper::SyncParam(config->GetFileCompressConfigV2(), segmentStatistics,
                                                              option);
        writer->SetWriterOption(option);
    }
    return writer;
}

DictionaryWriter*
DictionaryCreator::CreateWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                const std::string& temperatureLayer, autil::mem_pool::PoolBase* pool)
{
    const std::shared_ptr<config::FileCompressConfig>& fileCompressConfig = config->GetFileCompressConfig();
    DictionaryWriter* writer = nullptr;
    if (config->IsHashTypedDictionary()) {
        writer = DictionaryTypedFactory<DictionaryWriter, HashDictionaryWriter>::GetInstance()->Create(
            config->GetInvertedIndexType(), pool);
        if (fileCompressConfig) {
            AUTIL_LOG(WARN, "index [%s] with hash type dictionary will ignore file compress.",
                      config->GetIndexName().c_str());
        }
    } else {
        writer = DictionaryTypedFactory<DictionaryWriter, TieredDictionaryWriter>::GetInstance()->Create(
            config->GetInvertedIndexType(), pool);
        file_system::WriterOption option;
        indexlibv2::index::FileCompressParamHelper::SyncParam(fileCompressConfig, temperatureLayer, option);
        writer->SetWriterOption(option);
    }
    return writer;
}

DictionaryReader*
DictionaryCreator::CreateDiskReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config)
{
    if (config->IsHashTypedDictionary()) {
        return DictionaryTypedFactory<DictionaryReader, CommonDiskHashDictionaryReaderTyped>::GetInstance()->Create(
            config->GetInvertedIndexType());
    }
    return DictionaryTypedFactory<DictionaryReader, CommonDiskTieredDictionaryReaderTyped>::GetInstance()->Create(
        config->GetInvertedIndexType());
}

} // namespace indexlib::index
