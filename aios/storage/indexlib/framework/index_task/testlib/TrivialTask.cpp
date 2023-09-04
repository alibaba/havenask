#include "indexlib/framework/index_task/testlib/TrivialTask.h"

#include "indexlib/config/SchemaResolver.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/framework/DiskSegment.h"
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

std::unique_ptr<config::SchemaResolver> TrivialTaskTabletFactory::CreateSchemaResolver() const { return nullptr; }

std::unique_ptr<TabletWriter>
TrivialTaskTabletFactory::CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return nullptr;
}
std::unique_ptr<TabletReader>
TrivialTaskTabletFactory::CreateTabletReader(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return nullptr;
}
std::unique_ptr<ITabletLoader> TrivialTaskTabletFactory::CreateTabletLoader(const std::string& fenceName)
{
    return nullptr;
}
std::unique_ptr<DiskSegment> TrivialTaskTabletFactory::CreateDiskSegment(const SegmentMeta& segmentMeta,
                                                                         const framework::BuildResource& buildResource)
{
    return nullptr;
}
std::unique_ptr<MemSegment> TrivialTaskTabletFactory::CreateMemSegment(const SegmentMeta& segmentMeta)
{
    return nullptr;
}

// for index task
std::unique_ptr<IIndexTaskResourceCreator> TrivialTaskTabletFactory::CreateIndexTaskResourceCreator()
{
    return nullptr;
}

std::unique_ptr<IIndexOperationCreator>
TrivialTaskTabletFactory::CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<TrivialOperationCreator>(&_st);
}

std::unique_ptr<IIndexTaskPlanCreator> TrivialTaskTabletFactory::CreateIndexTaskPlanCreator() { return nullptr; }

std::unique_ptr<indexlib::framework::ITabletExporter> TrivialTaskTabletFactory::CreateTabletExporter()
{
    return nullptr;
}

std::unique_ptr<document::IDocumentFactory>
TrivialTaskTabletFactory::CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return {};
}

std::unique_ptr<ITabletImporter> TrivialTaskTabletFactory::CreateTabletImporter(const std::string& type)
{
    return nullptr;
}

std::unique_ptr<indexlib::framework::ITabletValidator> TrivialTaskTabletFactory::CreateTabletValidator()
{
    return nullptr;
}
} // namespace indexlibv2::framework::testlib
