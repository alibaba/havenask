#ifndef ISEARCH_BS_SUBDOCUMENTEXTRACTORPROCESSOR_H
#define ISEARCH_BS_SUBDOCUMENTEXTRACTORPROCESSOR_H

#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/SubDocumentExtractor.h"

namespace build_service {
namespace processor {

class SubDocumentExtractorProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    SubDocumentExtractorProcessor();
    ~SubDocumentExtractorProcessor();

public:
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();
    virtual bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader);

    virtual std::string getDocumentProcessorName() const
    { return PROCESSOR_NAME; }

private:
    SubDocumentExtractorPtr _subDocumentExtractor;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SubDocumentExtractorProcessor);

}
}

#endif //ISEARCH_BS_SUBDOCUMENTEXTRACTORPROCESSOR_H
