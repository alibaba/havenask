#ifndef ISEARCH_BS_RAWTEXTDOCUMENTPROCESSOR_H
#define ISEARCH_BS_RAWTEXTDOCUMENTPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include <indexlib/config/index_partition_schema.h>

namespace build_service {
namespace processor {

class LineDataDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    LineDataDocumentProcessor();
    LineDataDocumentProcessor(const LineDataDocumentProcessor& other);
    ~LineDataDocumentProcessor();
public:
    bool init(const DocProcessorInitParam &param) override;
    bool process(const document::ExtendDocumentPtr &document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs) override;

    void destroy()
    { delete this; }

    DocumentProcessor* clone()
    { return new LineDataDocumentProcessor(*this); }

    std::string getDocumentProcessorName() const
    { return PROCESSOR_NAME; }

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_RAWTEXTDOCUMENTPROCESSOR_H
