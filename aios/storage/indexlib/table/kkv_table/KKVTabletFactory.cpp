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
#include "indexlib/table/kkv_table/KKVTabletFactory.h"

#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/kkv/KKVDocumentFactory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/index/common/ShardPartitioner.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/common/CommonTabletValidator.h"
#include "indexlib/table/common/LSMTabletLoader.h"
#include "indexlib/table/kkv_table/KKVMemSegment.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "indexlib/table/kkv_table/KKVTabletExporter.h"
#include "indexlib/table/kkv_table/KKVTabletReader.h"
#include "indexlib/table/kkv_table/KKVTabletSessionReader.h"
#include "indexlib/table/kkv_table/KKVTabletWriter.h"
#include "indexlib/table/kkv_table/index_task/KKVTableTaskOperationCreator.h"
#include "indexlib/table/kkv_table/index_task/KKVTableTaskPlanCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableResourceCreator.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"
#include "indexlib/table/plain/PlainDiskSegment.h"
#include "indexlib/table/plain/PlainMemSegment.h"
#include "kmonitor/client/MetricsReporter.h"

using namespace indexlibv2::framework;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTabletFactory);

std::unique_ptr<ITabletFactory> KKVTabletFactory::Create() const { return std::make_unique<KKVTabletFactory>(); }

bool KKVTabletFactory::Init(const std::shared_ptr<config::TabletOptions>& options,
                            framework::MetricsManager* metricsManager)
{
    auto tmpOptions = std::make_unique<config::TabletOptions>(*options);
    _options.reset(new KKVTabletOptions(std::move(tmpOptions)));
    return true;
}

std::unique_ptr<config::SchemaResolver> KKVTabletFactory::CreateSchemaResolver() const
{
    return std::make_unique<KKVSchemaResolver>();
}

std::unique_ptr<framework::TabletWriter>
KKVTabletFactory::CreateTabletWriter(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<KKVTabletWriter>(schema, _options->GetTabletOptions());
}

std::unique_ptr<TabletReader> KKVTabletFactory::CreateTabletReader(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<KKVTabletReader>(schema);
}

std::shared_ptr<framework::ITabletReader>
KKVTabletFactory::CreateTabletSessionReader(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                            const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
{
    auto reader = std::dynamic_pointer_cast<KKVTabletReader>(tabletReader);
    if (nullptr == reader) {
        AUTIL_LOG(ERROR, "create session reader fail because of un-known input reader");
        return nullptr;
    }
    return std::make_shared<KKVTabletSessionReader>(reader, memReclaimer);
}

std::unique_ptr<ITabletLoader> KKVTabletFactory::CreateTabletLoader(const std::string& branchName)
{
    return std::make_unique<LSMTabletLoader>(std::move(branchName));
}

std::unique_ptr<framework::DiskSegment>
KKVTabletFactory::CreateDiskSegment(const SegmentMeta& segmentMeta, const framework::BuildResource& buildResource)
{
    return std::make_unique<plain::MultiShardDiskSegment>(segmentMeta.schema, segmentMeta, buildResource);
}

std::unique_ptr<framework::MemSegment> KKVTabletFactory::CreateMemSegment(const SegmentMeta& segmentMeta)
{
    auto segmentInfo = segmentMeta.segmentInfo;
    if (segmentInfo->GetShardCount() == framework::SegmentInfo::INVALID_COLUMN_COUNT) {
        segmentInfo->SetShardCount(_options->GetShardNum());
        AUTIL_LOG(INFO, "set shard count from[%u] to[%u] ", framework::SegmentInfo::INVALID_COLUMN_COUNT,
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

    uint32_t suggestLevelNum = _options->GetLevelNum();
    return std::make_unique<plain::MultiShardMemSegment>(partitioner, suggestLevelNum, _options->GetTabletOptions(),
                                                         segmentMeta.schema, segmentMeta,
                                                         KKVMemSegment::CreatePlainMemSegment);
}

std::unique_ptr<framework::IIndexTaskResourceCreator> KKVTabletFactory::CreateIndexTaskResourceCreator()
{
    return std::make_unique<KVTableResourceCreator>();
}

std::unique_ptr<framework::IIndexOperationCreator>
KKVTabletFactory::CreateIndexOperationCreator(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<KKVTableTaskOperationCreator>(schema);
}

std::unique_ptr<framework::IIndexTaskPlanCreator> KKVTabletFactory::CreateIndexTaskPlanCreator()
{
    return std::make_unique<KKVTableTaskPlanCreator>();
}

std::unique_ptr<indexlib::framework::ITabletExporter> KKVTabletFactory::CreateTabletExporter()
{
    return std::make_unique<indexlib::table::KKVTabletExporter>();
}

std::unique_ptr<document::IDocumentFactory>
KKVTabletFactory::CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<document::KKVDocumentFactory>();
}

std::unique_ptr<indexlib::framework::ITabletValidator> KKVTabletFactory::CreateTabletValidator()
{
    return std::make_unique<indexlib::table::CommonTabletValidator>();
}

REGISTER_FACTORY(kkv, KKVTabletFactory);

} // namespace indexlibv2::table
