#pragma once

#include <mutex>
#include <vector>

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

struct TrivialSharedState {
    std::mutex mtx;
    std::vector<IndexOperationId> executeIds;
};

class TrivialOperation : public IndexOperation
{
public:
    TrivialOperation(IndexOperationId id, TrivialSharedState* st) : IndexOperation(id, true), _id(id), _st(st) {}
    Status Execute(const IndexTaskContext&) override
    {
        std::lock_guard<std::mutex> lock(_st->mtx);
        _st->executeIds.push_back(_id);
        return Status::OK();
    }

private:
    IndexOperationId _id;
    TrivialSharedState* _st;
};

class TrivialOperationCreator : public IIndexOperationCreator
{
public:
    TrivialOperationCreator(TrivialSharedState* st) : _st(st) {}
    std::unique_ptr<IndexOperation> CreateOperation(const IndexOperationDescription& opDesc) override
    {
        return std::make_unique<TrivialOperation>(opDesc.GetId(), _st);
    }

private:
    TrivialSharedState* _st;
};

class TrivialTaskTabletFactory : public ITabletFactory
{
public:
    TrivialTaskTabletFactory() = default;
    ~TrivialTaskTabletFactory() = default;

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

private:
    TrivialSharedState _st;
};

} // namespace indexlibv2::framework::testlib
