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
#include "indexlib/index/kkv/KKVIndexFactory.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/merge/KKVMerger.h"
#include "indexlib/index/kkv/search/KKVIndexReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KKVIndexFactory);

std::shared_ptr<IDiskIndexer>
KKVIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    assert(indexerParam.segmentInfo);

    auto timestamp = indexerParam.segmentInfo->GetTimestamp();
    auto isRtSegment = framework::Segment::IsRtSegmentId(indexerParam.segmentId);

    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    if (!kkvIndexConfig) {
        AUTIL_LOG(ERROR, "expect KKVIndexConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }

    FieldType skeyDictType = kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_shared<KKVDiskIndexer<SKeyType>>(timestamp, isRtSegment);                                     \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

std::shared_ptr<IMemIndexer> KKVIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                               const IndexerParameter& indexerParam) const
{
    assert(indexerParam.tabletOptions);
    const std::string& tabletName = indexerParam.tabletOptions->GetTabletName();

    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    if (!kkvIndexConfig) {
        AUTIL_LOG(ERROR, "expect KKVIndexConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }
    bool isOnline = indexerParam.isOnline;
    int64_t maxBuildMemoryUse = GetMaxBuildMemoryUse(indexerParam);
    if (maxBuildMemoryUse <= 0) {
        AUTIL_LOG(ERROR, "invalid maxMemoryUse: %ld", indexerParam.maxMemoryUseInBytes);
        return nullptr;
    }

    double pkeyMemRatio = DEFAULT_PKEY_MEMORY_USE_RATIO;
    double skeyMemRatio = DEFAULT_SKEY_VALUE_MEM_RATIO;
    double skeyCompressRatio = DEFAULT_SKEY_COMPRESS_RATIO;
    double valueCompressRatio = DEFAULT_VALUE_COMPRESS_RATIO;

    auto lastSegmentMetrics = indexerParam.lastSegmentMetrics;
    if (lastSegmentMetrics != nullptr) {
        KKVSegmentStatistics stat;
        if (stat.Load(lastSegmentMetrics, indexConfig->GetIndexName())) {
            pkeyMemRatio = stat.pkeyMemoryRatio;
            CalculateMemIndexerSKeyMemRatio(skeyMemRatio, stat);
            skeyCompressRatio = stat.skeyCompressRatio;
            valueCompressRatio = stat.valueCompressRatio;
        }
    }

    size_t pkeyMemUse = maxBuildMemoryUse * pkeyMemRatio;
    size_t skeyMemUse = maxBuildMemoryUse * skeyMemRatio;
    size_t valueMemUse = maxBuildMemoryUse - skeyMemUse;

    AUTIL_LOG(
        INFO,
        "[%s] pkey size [%lu], skey size [%lu], value size [%lu], total size [%lu], pkey ratio [%f], skey ratio[%f]",
        tabletName.c_str(), pkeyMemUse, skeyMemUse, valueMemUse, maxBuildMemoryUse, pkeyMemRatio, skeyMemRatio);

    FieldType skeyDictType = kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_shared<KKVMemIndexer<SKeyType>>(tabletName, pkeyMemUse, skeyMemUse, valueMemUse,              \
                                                         skeyCompressRatio, valueCompressRatio, isOnline);             \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

std::unique_ptr<IIndexReader>
KKVIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    // TODO(chekong.ygm): need impl
    return std::make_unique<KKVIndexReader>();
}

std::unique_ptr<IIndexMerger>
KKVIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    if (!kkvIndexConfig) {
        AUTIL_LOG(ERROR, "expect KKVIndexConfig, actual: %s", indexConfig->GetIndexType().c_str());
        return nullptr;
    }

    FieldType skeyDictType = kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_unique<KKVMergerTyped<SKeyType>>();                                                           \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

std::unique_ptr<config::IIndexConfig> KKVIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    return std::make_unique<config::KKVIndexConfig>();
}

std::string KKVIndexFactory::GetIndexPath() const { return KKV_INDEX_PATH; }

void KKVIndexFactory::CalculateMemIndexerSKeyMemRatio(double& skeyMemRatio, const KKVSegmentStatistics& stat) const
{
    double prevSKeyMemoryRatio = stat.skeyValueMemoryRatio;
    size_t prevSKeyMemUse = stat.skeyMemoryUse;
    size_t prevValueMemUse = stat.valueMemoryUse;
    size_t prevTotalMemUse = prevSKeyMemUse + prevValueMemUse;
    double skeyNewMemoryRatioWeight = SKEY_NEW_MEMORY_RATIO_WEIGHT;
    if (prevTotalMemUse > 0) {
        skeyMemRatio = ((double)prevSKeyMemUse / prevTotalMemUse) * skeyNewMemoryRatioWeight +
                       prevSKeyMemoryRatio * (1 - skeyNewMemoryRatioWeight);
    }
    skeyMemRatio = std::max(MIN_SKEY_VALUE_MEM_RATIO, skeyMemRatio);
    skeyMemRatio = std::min(MAX_SKEY_VALUE_MEM_RATIO, skeyMemRatio);
    AUTIL_LOG(INFO,
              "Allocate SKey Memory by group Metrics, prevSKeyMem[%luB], prevValueMem[%luB], "
              "prevRatio[%.6f], skeyMemRatio[%.6f]",
              prevSKeyMemUse, prevValueMemUse, prevSKeyMemoryRatio, skeyMemRatio);
}

int64_t KKVIndexFactory::GetMaxBuildMemoryUse(const IndexerParameter& indexerParam) const
{
    uint32_t shardNum = (indexerParam.segmentInfo &&
                         indexerParam.segmentInfo->GetShardCount() != framework::SegmentInfo::INVALID_SHARDING_COUNT)
                            ? indexerParam.segmentInfo->GetShardCount()
                            : 1;
    // reserve 32M / shardNum for dump(compressBufferSize) & pool overhead at most
    const int64_t overhead = 32 * 1024 * 1024 / shardNum;
    if (indexerParam.maxMemoryUseInBytes > 2 * overhead) {
        return indexerParam.maxMemoryUseInBytes - overhead;
    }
    return indexerParam.maxMemoryUseInBytes / _memoryFactor;
}

REGISTER_INDEX_FACTORY(kkv, KKVIndexFactory);

} // namespace indexlibv2::index
