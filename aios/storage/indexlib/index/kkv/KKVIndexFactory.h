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
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/kkv/common/KKVSegmentStatistics.h"

namespace indexlibv2::index {

class KKVIndexFactory : public IIndexFactory
{
public:
    KKVIndexFactory() = default;
    ~KKVIndexFactory() = default;

public:
    std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override;
    std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                  const IndexerParameter& indexerParam) const override;
    std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override;
    std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const override;
    std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override;
    std::string GetIndexPath() const override;
    std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() override { return nullptr; }

private:
    void CalculateMemIndexerSKeyMemRatio(double& skeyMemRatio, const KKVSegmentStatistics& stat) const;

private:
    uint32_t _memoryFactor = 2;

private:
    // pkeyMem / totalMem
    static constexpr double DEFAULT_PKEY_MEMORY_USE_RATIO = 0.1;
    // skeyMem / (skeyMem + valueMem)
    static constexpr double DEFAULT_SKEY_VALUE_MEM_RATIO = 0.01;
    static constexpr double MIN_SKEY_VALUE_MEM_RATIO = 0.0001;
    static constexpr double MAX_SKEY_VALUE_MEM_RATIO = 0.90;
    static constexpr double SKEY_NEW_MEMORY_RATIO_WEIGHT = 0.7;
    static constexpr double DEFAULT_SKEY_COMPRESS_RATIO = 1.0;
    static constexpr double DEFAULT_VALUE_COMPRESS_RATIO = 1.0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
