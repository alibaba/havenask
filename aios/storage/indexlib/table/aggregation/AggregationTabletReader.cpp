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
#include "indexlib/table/aggregation/AggregationTabletReader.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/IndexerParameter.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AggregationTabletReader);

AggregationTabletReader::AggregationTabletReader(const std::shared_ptr<config::TabletSchema>& schema)
    : framework::TabletReader(schema)
{
}

AggregationTabletReader::~AggregationTabletReader() = default;

Status AggregationTabletReader::Open(const std::shared_ptr<framework::TabletData>& tabletData,
                                     const framework::ReadResource& readResource)
{
    index::IndexerParameter indexerParam;
    indexerParam.metricsManager = readResource.metricsManager;
    indexerParam.readerSchemaId = _schema->GetSchemaId();
    auto indexConfigs = _schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        const auto& indexType = indexConfig->GetIndexType();
        auto [s, indexFactory] = index::IndexFactoryCreator::GetInstance()->Create(indexType);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "failed to create index factory for %s", indexType.c_str());
            return s;
        }
        auto indexReader = indexFactory->CreateIndexReader(indexConfig, indexerParam);
        if (!indexReader) {
            return Status::InternalError("failed to CreateIndexReader for index type: %s", indexType.c_str());
        }
        s = indexReader->Open(indexConfig, tabletData.get());
        if (!s.IsOK()) {
            return s;
        }
        auto key = std::make_pair(indexType, indexConfig->GetIndexName());
        _indexReaderMap.emplace(std::move(key), std::move(indexReader));
    }
    return Status::OK();
}

} // namespace indexlibv2::table
