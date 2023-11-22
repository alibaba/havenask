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
#include "indexlib/index/common/Types.h"

namespace indexlibv2::index {
class IDiskIndexer;
class IMemIndexer;
class IIndexReader;
class IIndexMerger;
} // namespace indexlibv2::index
namespace indexlib::index {
class InvertedIndexos;

class InvertedIndexFactory : public indexlibv2::index::IIndexFactory
{
public:
    InvertedIndexFactory() = default;
    ~InvertedIndexFactory() = default;

    std::shared_ptr<indexlibv2::index::IDiskIndexer>
    CreateDiskIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                      const indexlibv2::index::DiskIndexerParameter& indexerParam) const override;
    std::shared_ptr<indexlibv2::index::IMemIndexer>
    CreateMemIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                     const indexlibv2::index::MemIndexerParameter& indexerParam) const override;
    std::unique_ptr<indexlibv2::index::IIndexReader>
    CreateIndexReader(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                      const indexlibv2::index::IndexReaderParameter& indexReaderParam) const override;
    std::unique_ptr<indexlibv2::index::IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const override;
    std::unique_ptr<indexlibv2::config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override;
    std::string GetIndexPath() const override;
    std::unique_ptr<indexlibv2::document::IIndexFieldsParser> CreateIndexFieldsParser() override;

private:
    bool CheckSupport(InvertedIndexType indexType) const;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
