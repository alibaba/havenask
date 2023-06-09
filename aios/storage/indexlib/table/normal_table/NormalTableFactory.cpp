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
#include "indexlib/table/normal_table/NormalTableFactory.h"

#include "indexlib/config/OnlineConfig.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocumentFactory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
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
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/common/CommonTabletValidator.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalDiskSegment.h"
#include "indexlib/table/normal_table/NormalMemSegment.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "indexlib/table/normal_table/NormalTabletExportLoader.h"
#include "indexlib/table/normal_table/NormalTabletExporter.h"
#include "indexlib/table/normal_table/NormalTabletLoader.h"
#include "indexlib/table/normal_table/NormalTabletMetrics.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "indexlib/table/normal_table/NormalTabletValidator.h"
#include "indexlib/table/normal_table/NormalTabletWriter.h"
#include "indexlib/table/normal_table/index_task/NormalTableResourceCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableTaskOperationCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableTaskPlanCreator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableFactory);

NormalTableFactory::NormalTableFactory() {}

NormalTableFactory::~NormalTableFactory() {}

std::unique_ptr<framework::ITabletFactory> NormalTableFactory::Create() const
{
    assert(!_options);
    return std::make_unique<NormalTableFactory>();
}

bool NormalTableFactory::Init(const std::shared_ptr<config::TabletOptions>& options,
                              framework::MetricsManager* metricsManager)
{
    _options.reset(new config::TabletOptions(*options));
    _normalTabletMetrics = std::make_shared<NormalTabletMetrics>(metricsManager);
    return true;
}

std::unique_ptr<config::SchemaResolver> NormalTableFactory::CreateSchemaResolver() const
{
    return std::make_unique<NormalSchemaResolver>(_options.get());
}

std::unique_ptr<framework::TabletWriter>
NormalTableFactory::CreateTabletWriter(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<NormalTabletWriter>(schema, _options.get());
}

std::unique_ptr<framework::TabletReader>
NormalTableFactory::CreateTabletReader(const std::shared_ptr<config::TabletSchema>& schema)
{
    if (_normalTabletMetrics->GetSchemaSignature() != schema.get()) {
        auto newNormalTabletMetrics = std::make_shared<NormalTabletMetrics>(*_normalTabletMetrics);
        newNormalTabletMetrics->Init(schema);
        _normalTabletMetrics = newNormalTabletMetrics;
    }
    return std::make_unique<NormalTabletReader>(schema, _normalTabletMetrics);
}

std::shared_ptr<framework::ITabletReader>
NormalTableFactory::CreateTabletSessionReader(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
{
    auto reader = std::static_pointer_cast<NormalTabletReader>(tabletReader);
    if (nullptr == reader) {
        AUTIL_LOG(ERROR, "create session reader fail because of un-known input reader");
        return nullptr;
    }
    return std::make_shared<NormalTabletSessionReader>(reader, memReclaimer);
}

std::unique_ptr<framework::ITabletLoader> NormalTableFactory::CreateTabletLoader(const std::string& branchName)
{
    bool isExportLoader = false;
    if (_options->GetFromRawJson("export_loader", &isExportLoader) && isExportLoader) {
        std::string workPath;
        if (_options->GetFromRawJson("export_loader_workpath", &workPath)) {
            return std::make_unique<indexlib::table::NormalTabletExportLoader>(branchName, workPath,
                                                                               std::nullopt /*targetSegmentIds*/);
        } else {
            AUTIL_LOG(ERROR, "create export loader fail, [export_loader_workpath] not set");
            return nullptr;
        }
    }

    bool loadIndexForCheck = false;
    if (!_options->GetFromRawJson("load_index_for_check", &loadIndexForCheck)) {
        loadIndexForCheck = false;
    }
    return std::make_unique<NormalTabletLoader>(
        std::move(branchName), _options->GetOnlineConfig().IsIncConsistentWithRealtime(), loadIndexForCheck);
}

std::unique_ptr<framework::DiskSegment>
NormalTableFactory::CreateDiskSegment(const framework::SegmentMeta& segmentMeta,
                                      const framework::BuildResource& buildResource)
{
    return std::make_unique<NormalDiskSegment>(segmentMeta.schema, segmentMeta, buildResource);
}

std::unique_ptr<framework::MemSegment> NormalTableFactory::CreateMemSegment(const framework::SegmentMeta& segmentMeta)
{
    return std::make_unique<NormalMemSegment>(_options.get(), segmentMeta.schema, segmentMeta);
}

std::unique_ptr<framework::IIndexTaskResourceCreator> NormalTableFactory::CreateIndexTaskResourceCreator()
{
    return std::make_unique<NormalTableResourceCreator>();
}

std::unique_ptr<framework::IIndexOperationCreator>
NormalTableFactory::CreateIndexOperationCreator(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<NormalTableTaskOperationCreator>(schema);
}

std::unique_ptr<framework::IIndexTaskPlanCreator> NormalTableFactory::CreateIndexTaskPlanCreator()
{
    return std::make_unique<NormalTableTaskPlanCreator>();
}

std::unique_ptr<indexlib::framework::ITabletExporter> NormalTableFactory::CreateTabletExporter()
{
    return std::make_unique<indexlib::table::NormalTabletExporter>();
}

const config::TabletOptions* NormalTableFactory::GetTabletOptions() const { return _options.get(); }

std::unique_ptr<document::IDocumentFactory>
NormalTableFactory::CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<document::NormalDocumentFactory>();
}

std::unique_ptr<indexlib::framework::ITabletValidator> NormalTableFactory::CreateTabletValidator()
{
    auto checkPk = autil::EnvUtil::getEnv<bool>("INDEXLIB_CHECK_PK_DUPLICATION", /*defaultValue*/ false);
    if (checkPk) {
        return std::make_unique<indexlib::table::NormalTabletValidator>();
    }
    return std::make_unique<indexlib::table::CommonTabletValidator>();
}

REGISTER_FACTORY(normal, NormalTableFactory);

} // namespace indexlibv2::table
