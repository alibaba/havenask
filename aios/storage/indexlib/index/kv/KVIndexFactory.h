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

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::index {

class KVIndexFactory : public IIndexFactory
{
public:
    KVIndexFactory();
    ~KVIndexFactory();

public:
    std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                  const IndexerParameter& indexerParam) const override;
    std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override;
    std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override;
    std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const override;
    std::string GetIndexPath() const override;
    std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override;
    std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() override;

private:
    std::shared_ptr<IMemIndexer>
    CreateFixedLenKVMemIndexer(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig,
                               const IndexerParameter& indexerParam) const;
    std::shared_ptr<IMemIndexer>
    CreateVarLenKVMemIndexer(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig,
                             const IndexerParameter& indexerParam) const;

public:
    void TEST_SetMemoryFactor(uint32_t memoryFactor) { _memoryFactor = memoryFactor; }

private:
    uint32_t _memoryFactor;
};

} // namespace indexlibv2::index
