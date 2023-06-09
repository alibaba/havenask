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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"
#include "indexlib/table/kkv_table/KKVTabletOptions.h"

namespace indexlibv2::framework {
struct SegmentMeta;
class TabletWriter;
class ITabletReader;
class TabletLoader;
class MemSegment;
class TabletData;
} // namespace indexlibv2::framework

namespace indexlibv2::table {

class KKVTabletFactory : public framework::ITabletFactory
{
public:
    std::unique_ptr<ITabletFactory> Create() const override;
    bool Init(const std::shared_ptr<config::TabletOptions>& options,
              framework::MetricsManager* metricsManager) override;

    std::unique_ptr<config::SchemaResolver> CreateSchemaResolver() const override;
    std::unique_ptr<framework::TabletWriter>
    CreateTabletWriter(const std::shared_ptr<config::TabletSchema>& schema) override;
    std::unique_ptr<framework::TabletReader>
    CreateTabletReader(const std::shared_ptr<config::TabletSchema>& schema) override;
    std::shared_ptr<framework::ITabletReader>
    CreateTabletSessionReader(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer) override;
    std::unique_ptr<framework::ITabletLoader> CreateTabletLoader(const std::string& branchName) override;
    std::unique_ptr<framework::DiskSegment> CreateDiskSegment(const framework::SegmentMeta& segmentMeta,
                                                              const framework::BuildResource& buildResource) override;
    std::unique_ptr<framework::MemSegment> CreateMemSegment(const framework::SegmentMeta& segmentMeta) override;

    std::unique_ptr<framework::IIndexTaskResourceCreator> CreateIndexTaskResourceCreator() override;
    std::unique_ptr<framework::IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::TabletSchema>& schema) override;
    std::unique_ptr<framework::IIndexTaskPlanCreator> CreateIndexTaskPlanCreator() override;
    std::unique_ptr<indexlib::framework::ITabletExporter> CreateTabletExporter() override;
    std::unique_ptr<document::IDocumentFactory>
    CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema) override;
    std::unique_ptr<indexlib::framework::ITabletValidator> CreateTabletValidator() override;

private:
    std::unique_ptr<KKVTabletOptions> _options;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
