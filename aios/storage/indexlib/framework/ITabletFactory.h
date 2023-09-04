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
#include <string>

#include "indexlib/base/Status.h"
#include "indexlib/framework/TabletFactoryCreator.h"

namespace indexlibv2::config {
class TabletOptions;
class ITabletSchema;
class SchemaResolver;
} // namespace indexlibv2::config
namespace indexlib::framework {
class ITabletExporter;
class ITabletValidator;
} // namespace indexlib::framework
namespace indexlibv2::document {
class IDocumentFactory;
} // namespace indexlibv2::document

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
class ITabletReader;
class TabletWriter;
class TabletReader;
class ITabletLoader;
class DiskSegment;
class MemSegment;
struct SegmentMeta;
struct BuildResource;
class IIndexTaskResourceCreator;
class IIndexOperationCreator;
class IIndexTaskPlanCreator;
class MetricsManager;
class ITabletImporter;

class ITabletFactory
{
public:
    virtual ~ITabletFactory() = default;

    virtual std::unique_ptr<ITabletFactory> Create() const = 0;
    virtual bool Init(const std::shared_ptr<config::TabletOptions>& options, MetricsManager* metricsManager) = 0;
    virtual std::unique_ptr<config::SchemaResolver> CreateSchemaResolver() const = 0;

    // basic components
    virtual std::unique_ptr<TabletWriter> CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema) = 0;
    virtual std::unique_ptr<TabletReader> CreateTabletReader(const std::shared_ptr<config::ITabletSchema>& schema) = 0;
    virtual std::shared_ptr<ITabletReader>
    CreateTabletSessionReader(const std::shared_ptr<ITabletReader>& tabletReader,
                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer) = 0;
    virtual std::unique_ptr<ITabletLoader> CreateTabletLoader(const std::string& fenceName) = 0;
    virtual std::unique_ptr<DiskSegment> CreateDiskSegment(const SegmentMeta& segmentMeta,
                                                           const framework::BuildResource& buildResource) = 0;
    virtual std::unique_ptr<MemSegment> CreateMemSegment(const SegmentMeta& segmentMeta) = 0;
    virtual std::unique_ptr<document::IDocumentFactory>
    CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema) = 0;

    // index task components
    virtual std::unique_ptr<IIndexTaskResourceCreator> CreateIndexTaskResourceCreator() = 0;
    virtual std::unique_ptr<IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema) = 0;
    virtual std::unique_ptr<IIndexTaskPlanCreator> CreateIndexTaskPlanCreator() = 0;
    virtual std::unique_ptr<indexlib::framework::ITabletExporter> CreateTabletExporter() = 0;
    virtual std::unique_ptr<indexlib::framework::ITabletValidator> CreateTabletValidator() = 0;
    virtual std::unique_ptr<ITabletImporter> CreateTabletImporter(const std::string& type) = 0;
};

/*
   添加一种新的FACTORY，需要完成以下2个步骤:
   1.在你的factory cpp内调用 REGISTER_TABLET_FACTORY(table_type, ClassName); 的宏，
       例如， REGISTER_TABLET_FACTORY(normal, NormalTableFactory);
   2.在该文件对应的目标BUILD目标上添加    alwayslink = True
       可参考 NormalTableFactory  和对应的BUILD文件
*/

#define REGISTER_TABLET_FACTORY(TABLE_TYPE, TABLET_FACTORY)                                                            \
    __attribute__((constructor)) void Register##TABLE_TYPE##Factory()                                                  \
    {                                                                                                                  \
        auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();                        \
        tabletFactoryCreator->Register(#TABLE_TYPE, (new TABLET_FACTORY));                                             \
    }

} // namespace indexlibv2::framework
