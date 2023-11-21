#include "indexlib/framework/index_task/testlib/FakeTask.h"

#include "indexlib/config/SchemaResolver.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/EnvironmentVariablesProvider.h"
#include "indexlib/framework/ITabletExporter.h"
#include "indexlib/framework/ITabletImporter.h"
#include "indexlib/framework/ITabletValidator.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/TabletWriter.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"

namespace indexlibv2::framework::testlib {

std::unique_ptr<config::SchemaResolver> FakeTaskTabletFactory::CreateSchemaResolver() const { return nullptr; }

std::unique_ptr<TabletWriter>
FakeTaskTabletFactory::CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return nullptr;
}
std::unique_ptr<TabletReader>
FakeTaskTabletFactory::CreateTabletReader(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return nullptr;
}
std::unique_ptr<ITabletLoader> FakeTaskTabletFactory::CreateTabletLoader(const std::string& fenceName)
{
    return nullptr;
}
std::unique_ptr<DiskSegment> FakeTaskTabletFactory::CreateDiskSegment(const SegmentMeta& segmentMeta,
                                                                      const framework::BuildResource& buildResource)
{
    return nullptr;
}
std::unique_ptr<MemSegment> FakeTaskTabletFactory::CreateMemSegment(const SegmentMeta& segmentMeta) { return nullptr; }

// for index task
std::unique_ptr<IIndexTaskResourceCreator> FakeTaskTabletFactory::CreateIndexTaskResourceCreator() { return nullptr; }

std::unique_ptr<IIndexOperationCreator>
FakeTaskTabletFactory::CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<FakeOperationCreator>(&_st);
}

std::unique_ptr<IIndexTaskPlanCreator> FakeTaskTabletFactory::CreateIndexTaskPlanCreator() { return nullptr; }

std::unique_ptr<indexlib::framework::ITabletExporter> FakeTaskTabletFactory::CreateTabletExporter() { return nullptr; }

std::unique_ptr<document::IDocumentFactory>
FakeTaskTabletFactory::CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return {};
}

std::unique_ptr<ITabletImporter> FakeTaskTabletFactory::CreateTabletImporter(const std::string& type)
{
    return nullptr;
}

std::unique_ptr<indexlib::framework::ITabletValidator> FakeTaskTabletFactory::CreateTabletValidator()
{
    return nullptr;
}

std::unique_ptr<framework::IMemoryControlStrategy> FakeTaskTabletFactory::CreateMemoryControlStrategy(
    const std::shared_ptr<MemoryQuotaSynchronizer>& buildMemoryQuotaSynchronizer)
{
    return nullptr;
}

std::unique_ptr<indexlibv2::framework::EnvironmentVariablesProvider>
FakeTaskTabletFactory::CreateEnvironmentVariablesProvider()
{
    return nullptr;
}
} // namespace indexlibv2::framework::testlib
