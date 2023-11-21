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

namespace indexlibv2::index {

class SourceIndexFactory : public IIndexFactory
{
public:
    SourceIndexFactory();
    ~SourceIndexFactory();

public:
    std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const DiskIndexerParameter& indexerParam) const override;
    std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                  const MemIndexerParameter& indexerParam) const override;
    std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexReaderParameter& indexReaderParam) const override;
    std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const override;
    std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override;
    std::string GetIndexPath() const override;
    std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() override { return nullptr; }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
