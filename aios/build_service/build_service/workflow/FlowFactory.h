#ifndef ISEARCH_BS_FLOWFACTORY_H
#define ISEARCH_BS_FLOWFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BrokerFactory.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/workflow/RawDocBuilderConsumer.h"
#include <indexlib/partition/index_partition.h>

namespace build_service {
namespace workflow {

class FlowFactory : public BrokerFactory
{
public:
    FlowFactory(const util::SwiftClientCreatorPtr& swiftClientCreator,
        const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart =
        IE_NAMESPACE(partition)::IndexPartitionPtr());
    ~FlowFactory();
private:
    FlowFactory(const FlowFactory &);
    FlowFactory& operator=(const FlowFactory &);
public:
    RawDocProducer *createRawDocProducer(const RoleInitParam &initParam) override;
    RawDocConsumer *createRawDocConsumer(const RoleInitParam &initParam) override;

    ProcessedDocProducer *createProcessedDocProducer(const RoleInitParam &initParam) override;
    ProcessedDocConsumer *createProcessedDocConsumer(const RoleInitParam &initParam) override;
    bool initCounterMap(RoleInitParam &initParam) override;

public:
    reader::RawDocumentReader *getReader() const { return _reader; }
    processor::Processor *getProcessor() const { return _processor; }
    builder::Builder *getBuilder() const { return _builder; }
    void collectErrorInfos(std::vector<proto::ErrorInfo> &errorInfos) const override;
private:
    reader::RawDocumentReader *getReader(
            const RoleInitParam &initParam);
    processor::Processor *getProcessor(
            const RoleInitParam &initParam);
    builder::Builder *getBuilder(
            const RoleInitParam &initParam);
private:
    virtual reader::RawDocumentReader *createReader(
            const RoleInitParam &initParam);
    virtual processor::Processor *createProcessor(
            const RoleInitParam &initParam);
    virtual builder::Builder *createBuilder(
            const RoleInitParam &initParam);
    RawDocConsumer *createRawDocBuilderConsumer(const RoleInitParam &initParam);
    bool needProcessRawdoc(const RoleInitParam &initParam);

private:
    IE_NAMESPACE(partition)::IndexPartitionPtr _indexPart;
    reader::RawDocumentReaderCreator _readerCreator;
    reader::RawDocumentReader *_reader;
    processor::Processor *_processor;
    builder::Builder *_builder;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FlowFactory);

}
}

#endif //ISEARCH_BS_FLOWFACTORY_H
