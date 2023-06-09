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
#include "indexlib/table/aggregation/AggregationTabletFactory.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/aggregation/AggregationDocumentFactory.h"
#include "indexlib/framework/ITabletExporter.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/table/aggregation/AggregationSchemaResolver.h"
#include "indexlib/table/aggregation/AggregationSchemaUtil.h"
#include "indexlib/table/aggregation/AggregationTabletReader.h"
#include "indexlib/table/aggregation/AggregationTabletWriter.h"
#include "indexlib/table/aggregation/index_task/AggregationResourceCreator.h"
#include "indexlib/table/aggregation/index_task/AggregationTaskOperationCreator.h"
#include "indexlib/table/aggregation/index_task/AggregationTaskPlanCreator.h"
#include "indexlib/table/common/CommonTabletSessionReader.h"
#include "indexlib/table/common/CommonTabletValidator.h"
#include "indexlib/table/common/CommonTabletWriter.h"
#include "indexlib/table/common/LSMTabletLoader.h"
#include "indexlib/table/plain/SingleShardDiskSegment.h"
#include "indexlib/table/plain/SingleShardMemSegment.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AggregationTabletFactory);

static constexpr uint32_t LEVEL_NUM = 2;

AggregationTabletFactory::AggregationTabletFactory() = default;

AggregationTabletFactory::~AggregationTabletFactory() = default;

std::unique_ptr<framework::ITabletFactory> AggregationTabletFactory::Create() const
{
    return std::make_unique<AggregationTabletFactory>();
}

bool AggregationTabletFactory::Init(const std::shared_ptr<config::TabletOptions>& options,
                                    framework::MetricsManager* metricsManager)
{
    if (!options) {
        return false;
    }
    _tabletOptions = std::make_unique<config::TabletOptions>(*options);
    return true;
}

std::unique_ptr<config::SchemaResolver> AggregationTabletFactory::CreateSchemaResolver() const
{
    return std::make_unique<AggregationSchemaResolver>();
}

std::unique_ptr<framework::TabletWriter>
AggregationTabletFactory::CreateTabletWriter(const std::shared_ptr<config::TabletSchema>& schema)
{
    auto r = AggregationSchemaUtil::IsUniqueEnabled(schema.get());
    if (!r.IsOK()) {
        AUTIL_LOG(ERROR, "parse schema settings failed, error:%s", r.get_error().message().c_str());
        return nullptr;
    }
    bool enableUnique = r.get();
    if (!enableUnique) {
        return std::make_unique<CommonTabletWriter>(schema, _tabletOptions.get());
    } else {
        return std::make_unique<AggregationTabletWriter>(schema, _tabletOptions.get());
    }
}

std::unique_ptr<framework::TabletReader>
AggregationTabletFactory::CreateTabletReader(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<AggregationTabletReader>(schema);
}

std::shared_ptr<framework::ITabletReader> AggregationTabletFactory::CreateTabletSessionReader(
    const std::shared_ptr<framework::ITabletReader>& tabletReader,
    const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
{
    auto aggTabletReader = std::dynamic_pointer_cast<AggregationTabletReader>(tabletReader);
    if (!aggTabletReader) {
        return nullptr;
    }
    using SessionReader = CommonTabletSessionReader<AggregationTabletReader>;
    return std::make_shared<SessionReader>(aggTabletReader, memReclaimer);
}

std::unique_ptr<framework::ITabletLoader> AggregationTabletFactory::CreateTabletLoader(const std::string& fenceName)
{
    return std::make_unique<LSMTabletLoader>(fenceName);
}

std::unique_ptr<framework::DiskSegment>
AggregationTabletFactory::CreateDiskSegment(const framework::SegmentMeta& segmentMeta,
                                            const framework::BuildResource& buildResource)
{
    return std::make_unique<plain::SingleShardDiskSegment>(segmentMeta.schema, segmentMeta, buildResource);
}

std::unique_ptr<framework::MemSegment>
AggregationTabletFactory::CreateMemSegment(const framework::SegmentMeta& segmentMeta)
{
    auto segmentInfo = segmentMeta.segmentInfo;
    if (segmentInfo->GetShardCount() == framework::SegmentInfo::INVALID_COLUMN_COUNT) {
        segmentInfo->SetShardCount(1);
        AUTIL_LOG(INFO, "set shard count from[%u] to[%u] ", framework::SegmentInfo::INVALID_COLUMN_COUNT, 1);
    }
    assert(segmentInfo->GetShardCount() == 1);
    return std::make_unique<plain::SingleShardMemSegment>(_tabletOptions.get(), segmentMeta.schema, segmentMeta,
                                                          LEVEL_NUM);
}

// index task components
std::unique_ptr<framework::IIndexTaskResourceCreator> AggregationTabletFactory::CreateIndexTaskResourceCreator()
{
    return std::make_unique<AggregationResourceCreator>();
}

std::unique_ptr<framework::IIndexOperationCreator>
AggregationTabletFactory::CreateIndexOperationCreator(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<AggregationTaskOperationCreator>(schema);
}

std::unique_ptr<framework::IIndexTaskPlanCreator> AggregationTabletFactory::CreateIndexTaskPlanCreator()
{
    return std::make_unique<AggregationTaskPlanCreator>();
}

std::unique_ptr<indexlib::framework::ITabletExporter> AggregationTabletFactory::CreateTabletExporter()
{
    return nullptr;
}

std::unique_ptr<document::IDocumentFactory>
AggregationTabletFactory::CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<document::AggregationDocumentFactory>();
}

std::unique_ptr<indexlib::framework::ITabletValidator> AggregationTabletFactory::CreateTabletValidator()
{
    return std::make_unique<indexlib::table::CommonTabletValidator>();
}

REGISTER_FACTORY(aggregation, AggregationTabletFactory);

} // namespace indexlibv2::table
