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
#include "indexlib/index/kv/KVIndexFactory.h"

#include "autil/Log.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/kv/FixedLenKVMemIndexer.h"
#include "indexlib/index/kv/FixedLenKVMerger.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVIndexFieldsParser.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/SingleShardKVIndexReader.h"
#include "indexlib/index/kv/VarLenKVMemIndexer.h"
#include "indexlib/index/kv/VarLenKVMerger.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVIndexFactory);

// TODO: by yijie.zhang set memory factor = 1 when use buffer file writer
KVIndexFactory::KVIndexFactory() : _memoryFactor(2) {}

KVIndexFactory::~KVIndexFactory() {}

std::shared_ptr<IMemIndexer> KVIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                              const IndexerParameter& indexerParam) const
{
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!kvIndexConfig) {
        AUTIL_LOG(ERROR, "expect KVIndexConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }

    int64_t maxBuildMemoryUse = indexerParam.maxMemoryUseInBytes / _memoryFactor;
    if (maxBuildMemoryUse <= 0) {
        AUTIL_LOG(ERROR, "invalid maxMemoryUse: %ld", indexerParam.maxMemoryUseInBytes);
        return nullptr;
    }
    auto typeId = MakeKVTypeId(*kvIndexConfig, nullptr);
    if (typeId.isVarLen) {
        return CreateVarLenKVMemIndexer(kvIndexConfig, indexerParam);
    } else {
        return std::make_shared<FixedLenKVMemIndexer>(maxBuildMemoryUse);
    }
}

string KVIndexFactory::GetIndexPath() const { return KV_INDEX_PATH; }

std::shared_ptr<IMemIndexer>
KVIndexFactory::CreateVarLenKVMemIndexer(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig,
                                         const IndexerParameter& indexerParam) const
{
    float keyValueMemRatio = VarLenKVMemIndexer::DEFAULT_KEY_VALUE_MEM_RATIO;
    float valueCompressRatio = VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO;
    if (indexerParam.lastSegmentMetrics != nullptr) {
        SegmentStatistics stat;
        if (stat.Load(indexerParam.lastSegmentMetrics, indexConfig->GetIndexName())) {
            keyValueMemRatio = stat.keyValueSizeRatio;
            keyValueMemRatio = std::max(VarLenKVMemIndexer::MIN_KEY_VALUE_MEM_RATIO, keyValueMemRatio);
            keyValueMemRatio = std::min(VarLenKVMemIndexer::MAX_KEY_VALUE_MEM_RATIO, keyValueMemRatio);
            valueCompressRatio = stat.valueCompressRatio;
        }
    }

    // TODO: delete when use buffer file writer
    // half memory for dump
    int64_t maxBuildMemoryUse = indexerParam.maxMemoryUseInBytes / _memoryFactor;
    return std::make_shared<VarLenKVMemIndexer>(maxBuildMemoryUse, keyValueMemRatio, valueCompressRatio,
                                                indexerParam.sortDescriptions, indexerParam.indexMemoryReclaimer);
}

std::shared_ptr<IDiskIndexer>
KVIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const IndexerParameter& indexerParam) const
{
    return std::make_shared<KVDiskIndexer>();
}

std::unique_ptr<IIndexReader>
KVIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const IndexerParameter& indexerParam) const
{
    return std::make_unique<SingleShardKVIndexReader>(indexerParam.readerSchemaId);
}

std::unique_ptr<IIndexMerger>
KVIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!kvIndexConfig) {
        AUTIL_LOG(ERROR, "expect KVIndexConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }
    auto typeId = MakeKVTypeId(*kvIndexConfig, nullptr);
    if (typeId.isVarLen) {
        return std::make_unique<VarLenKVMerger>();
    } else {
        return std::make_unique<FixedLenKVMerger>();
    }
}

std::unique_ptr<config::IIndexConfig> KVIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::KVIndexConfig>();
}

std::unique_ptr<document::IIndexFieldsParser> KVIndexFactory::CreateIndexFieldsParser()
{
    return std::make_unique<KVIndexFieldsParser>();
}

REGISTER_INDEX(kv, KVIndexFactory);
} // namespace indexlibv2::index
