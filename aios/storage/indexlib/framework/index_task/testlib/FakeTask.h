#pragma once

#include <mutex>
#include <vector>

#include "indexlib/framework/IMemoryControlStrategy.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperation.h"

namespace indexlibv2::config {
class ITabletSchema;
class TabletOptions;
} // namespace indexlibv2::config
namespace indexlibv2::framework {
class TabletWriter;
class TabletReader;
class ITabletLoader;
class DiskSegment;
class MemSegment;
class IIndexTaskResourceCreator;
class IIndexTaskPlanCreator;
class ITabletDocIterator;
} // namespace indexlibv2::framework

namespace indexlibv2::framework::testlib {

struct FakeSharedState {
    std::mutex mtx;
    std::vector<IndexOperationId> executeIds;
};

class FakeOperation : public IndexOperation
{
public:
    FakeOperation(IndexOperationId id, FakeSharedState* st) : IndexOperation(id, true), _id(id), _st(st) {}
    Status Execute(const IndexTaskContext&) override
    {
        std::lock_guard<std::mutex> lock(_st->mtx);
        _st->executeIds.push_back(_id);
        return Status::OK();
    }

private:
    IndexOperationId _id;
    FakeSharedState* _st;
};

class FakeOperationCreator : public IIndexOperationCreator
{
public:
    FakeOperationCreator(FakeSharedState* st) : _st(st) {}
    std::unique_ptr<IndexOperation> CreateOperation(const IndexOperationDescription& opDesc) override
    {
        return std::make_unique<FakeOperation>(opDesc.GetId(), _st);
    }

private:
    FakeSharedState* _st;
};

class FakeTaskTabletFactory : public ITabletFactory
{
public:
    FakeTaskTabletFactory() = default;
    ~FakeTaskTabletFactory() = default;

    std::unique_ptr<ITabletFactory> Create() const override { return nullptr; }
    bool Init(const std::shared_ptr<config::TabletOptions>& options, MetricsManager* metricsManager) override
    {
        return false;
    }

    std::unique_ptr<config::SchemaResolver> CreateSchemaResolver() const override;

    std::unique_ptr<TabletWriter> CreateTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema) override;
    std::shared_ptr<ITabletReader>
    CreateTabletSessionReader(const std::shared_ptr<ITabletReader>& tabletReader,
                              const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer) override
    {
        return tabletReader;
    }
    std::unique_ptr<TabletReader> CreateTabletReader(const std::shared_ptr<config::ITabletSchema>& schema) override;
    std::unique_ptr<ITabletLoader> CreateTabletLoader(const std::string& fenceName) override;
    std::unique_ptr<DiskSegment> CreateDiskSegment(const SegmentMeta& segmentMeta,
                                                   const framework::BuildResource& buildResource) override;
    std::unique_ptr<MemSegment> CreateMemSegment(const SegmentMeta& segmentMeta) override;

    // for index task
    std::unique_ptr<IIndexTaskResourceCreator> CreateIndexTaskResourceCreator() override;
    std::unique_ptr<IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema) override;
    std::unique_ptr<IIndexTaskPlanCreator> CreateIndexTaskPlanCreator() override;

    std::unique_ptr<indexlib::framework::ITabletExporter> CreateTabletExporter() override;
    std::unique_ptr<document::IDocumentFactory>
    CreateDocumentFactory(const std::shared_ptr<config::ITabletSchema>& schema) override;
    std::unique_ptr<indexlib::framework::ITabletValidator> CreateTabletValidator() override;
    std::unique_ptr<framework::ITabletImporter> CreateTabletImporter(const std::string& type) override;
    std::unique_ptr<framework::IMemoryControlStrategy>
    CreateMemoryControlStrategy(const std::shared_ptr<MemoryQuotaSynchronizer>& buildMemoryQuotaSynchronizer) override;
    std::unique_ptr<framework::EnvironmentVariablesProvider> CreateEnvironmentVariablesProvider() override;

private:
    FakeSharedState _st;
};

} // namespace indexlibv2::framework::testlib
