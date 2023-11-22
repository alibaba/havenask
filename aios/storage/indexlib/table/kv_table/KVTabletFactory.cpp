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
#include "indexlib/table/kv_table/KVTabletFactory.h"

#include "autil/EnvUtil.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/kv/KVDocumentFactory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/EnvironmentVariablesProvider.h"
#include "indexlib/framework/IMemoryControlStrategy.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/index/common/ShardPartitioner.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/common/CommonTabletValidator.h"
#include "indexlib/table/common/CommonVersionImporter.h"
#include "indexlib/table/common/LSMTabletLoader.h"
#include "indexlib/table/kv_table/KVMemSegment.h"
#include "indexlib/table/kv_table/KVReaderImpl.h"
#include "indexlib/table/kv_table/KVSchemaResolver.h"
#include "indexlib/table/kv_table/KVTabletExporter.h"
#include "indexlib/table/kv_table/KVTabletReader.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "indexlib/table/kv_table/KVTabletWriter.h"
#include "indexlib/table/kv_table/index_task/KVTableResourceCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableTaskOperationCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableTaskPlanCreator.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"
#include "indexlib/table/plain/PlainDiskSegment.h"
#include "indexlib/table/plain/PlainMemSegment.h"
#include "kmonitor/client/MetricsReporter.h"

using namespace indexlibv2::framework;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTabletFactory);

KVTabletFactory::KVTabletFactory() {}
KVTabletFactory::~KVTabletFactory() {}

std::unique_ptr<ITabletFactory> KVTabletFactory::Create() const { return std::make_unique<KVTabletFactory>(); }

bool KVTabletFactory::Init(const std::shared_ptr<config::TabletOptions>& options,
                           framework::MetricsManager* metricsManager)
{
    auto tmpOptions = std::make_unique<config::TabletOptions>(*options);
    _options.reset(new KVTabletOptions(std::move(tmpOptions)));
    return true;
}

std::unique_ptr<framework::TabletWriter>
KVTabletFactory::CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<KVTabletWriter>(schema, _options->GetTabletOptions());
}

std::unique_ptr<TabletReader> KVTabletFactory::CreateTabletReader(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<KVTabletReader>(schema);
}

std::shared_ptr<framework::ITabletReader>
KVTabletFactory::CreateTabletSessionReader(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                           const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
{
    auto reader = std::dynamic_pointer_cast<KVTabletReader>(tabletReader);
    if (nullptr == reader) {
        AUTIL_LOG(ERROR, "create session reader fail because of un-known input reader");
        return nullptr;
    }
    return std::make_shared<KVTabletSessionReader>(reader, memReclaimer);
}

std::unique_ptr<ITabletLoader> KVTabletFactory::CreateTabletLoader(const std::string& branchName)
{
    return std::make_unique<LSMTabletLoader>(std::move(branchName));
}

std::unique_ptr<framework::DiskSegment>
KVTabletFactory::CreateDiskSegment(const SegmentMeta& segmentMeta, const framework::BuildResource& buildResource)
{
    return std::make_unique<plain::MultiShardDiskSegment>(segmentMeta.schema, segmentMeta, buildResource);
}

std::unique_ptr<framework::MemSegment> KVTabletFactory::CreateMemSegment(const SegmentMeta& segmentMeta)
{
    auto segmentInfo = segmentMeta.segmentInfo;
    if (segmentInfo->GetShardCount() == framework::SegmentInfo::INVALID_SHARDING_COUNT) {
        segmentInfo->SetShardCount(_options->GetShardNum());
        AUTIL_LOG(INFO, "set shard count from[%u] to[%u] ", framework::SegmentInfo::INVALID_SHARDING_COUNT,
                  _options->GetShardNum());
    }

    std::shared_ptr<index::ShardPartitioner> partitioner;
    uint32_t shardCount = segmentInfo->GetShardCount();
    if (shardCount > 1) {
        partitioner = std::make_shared<index::ShardPartitioner>();
        const auto& status = partitioner->Init(shardCount);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "failed to init partitioner with error:[%s]", status.ToString().c_str());
            return nullptr;
        }
    }

    bool enableMemoryReclaim = _options->IsMemoryReclaimEnabled();
    auto segmentCreator = [enableMemoryReclaim](const config::TabletOptions* tableOptions,
                                                const std::shared_ptr<config::ITabletSchema>& schema,
                                                const framework::SegmentMeta& segmentMeta) {
        return std::make_shared<KVMemSegment>(tableOptions, schema, segmentMeta, enableMemoryReclaim);
    };
    uint32_t suggestLevelNum = _options->GetLevelNum();

    return std::make_unique<plain::MultiShardMemSegment>(partitioner, suggestLevelNum, _options->GetTabletOptions(),
                                                         segmentMeta.schema, segmentMeta, segmentCreator);
}

std::unique_ptr<framework::IIndexTaskResourceCreator> KVTabletFactory::CreateIndexTaskResourceCreator()
{
    return std::make_unique<KVTableResourceCreator>();
}

std::unique_ptr<framework::IIndexOperationCreator>
KVTabletFactory::CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<KVTableTaskOperationCreator>(schema);
}

std::unique_ptr<framework::IIndexTaskPlanCreator> KVTabletFactory::CreateIndexTaskPlanCreator()
{
    return std::make_unique<KVTableTaskPlanCreator>();
}

std::unique_ptr<indexlib::framework::ITabletExporter> KVTabletFactory::CreateTabletExporter()
{
    return std::make_unique<indexlib::table::KVTabletExporter>();
}

std::unique_ptr<document::IDocumentFactory>
KVTabletFactory::CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<document::KVDocumentFactory>();
}

std::unique_ptr<framework::ITabletImporter> KVTabletFactory::CreateTabletImporter(const std::string& type)
{
    return std::make_unique<table::CommonVersionImporter>();
}

std::unique_ptr<config::SchemaResolver> KVTabletFactory::CreateSchemaResolver() const
{
    return std::make_unique<KVSchemaResolver>();
}

std::unique_ptr<indexlib::framework::ITabletValidator> KVTabletFactory::CreateTabletValidator()
{
    return std::make_unique<indexlib::table::CommonTabletValidator>();
}

std::unique_ptr<framework::IMemoryControlStrategy> KVTabletFactory::CreateMemoryControlStrategy(
    const std::shared_ptr<MemoryQuotaSynchronizer>& buildMemoryQuotaSynchronizer)
{
    return nullptr;
}

std::unique_ptr<framework::EnvironmentVariablesProvider> KVTabletFactory::CreateEnvironmentVariablesProvider()
{
    return nullptr;
}

REGISTER_TABLET_FACTORY(kv, KVTabletFactory);

} // namespace indexlibv2::table
