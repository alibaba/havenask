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
#include "gmock/gmock.h"
#include <memory>
#include <string>

#include "indexlib/config/SchemaResolver.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/ITabletExporter.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/ITabletImporter.h"
#include "indexlib/framework/ITabletValidator.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"
#include "indexlib/framework/mock/MockSchemaResolver.h"

namespace indexlibv2::framework {

class MockTabletFactory : public ITabletFactory
{
public:
    std::unique_ptr<ITabletFactory> Create() const override { return std::make_unique<MockTabletFactory>(); }
    bool Init(const std::shared_ptr<config::TabletOptions>& options, MetricsManager* metricsManager) override
    {
        return true;
    }
    std::unique_ptr<config::SchemaResolver> CreateSchemaResolver() const override
    {
        return std::make_unique<MockSchemaResolver>();
    }
    std::unique_ptr<document::IDocumentFactory>
    CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return nullptr;
    }

public:
    MOCK_METHOD(std::unique_ptr<TabletWriter>, CreateTabletWriter,
                (const std::shared_ptr<config::ITabletSchema>& schema), (override));
    MOCK_METHOD(std::unique_ptr<TabletReader>, CreateTabletReader,
                (const std::shared_ptr<config::ITabletSchema>& schema), (override));
    MOCK_METHOD(std::shared_ptr<ITabletReader>, CreateTabletSessionReader,
                (const std::shared_ptr<ITabletReader>& tabletReader,
                 const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer),
                (override));
    MOCK_METHOD(std::unique_ptr<ITabletLoader>, CreateTabletLoader, (const std::string&), (override));
    MOCK_METHOD(std::unique_ptr<DiskSegment>, CreateDiskSegment,
                (const SegmentMeta& segmentMeta, const framework::BuildResource& buildResource), (override));
    MOCK_METHOD(std::unique_ptr<MemSegment>, CreateMemSegment, (const SegmentMeta& segmentMeta), (override));

    MOCK_METHOD(std::unique_ptr<IIndexTaskResourceCreator>, CreateIndexTaskResourceCreator, (), (override));
    MOCK_METHOD(std::unique_ptr<IIndexOperationCreator>, CreateIndexOperationCreator,
                (const std::shared_ptr<config::ITabletSchema>& schema), (override));
    MOCK_METHOD(std::unique_ptr<IIndexTaskPlanCreator>, CreateIndexTaskPlanCreator, (), (override));
    MOCK_METHOD(std::unique_ptr<indexlib::framework::ITabletExporter>, CreateTabletExporter, (), (override));
    MOCK_METHOD(std::unique_ptr<indexlib::framework::ITabletValidator>, CreateTabletValidator, (), (override));
    MOCK_METHOD(std::unique_ptr<ITabletImporter>, CreateTabletImporter, (const std::string& type), (override));
};
} // namespace indexlibv2::framework
