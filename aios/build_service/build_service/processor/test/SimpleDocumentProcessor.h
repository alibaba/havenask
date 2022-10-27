#ifndef __BUILD_DOCUMENT_PROCESSOR_H
#define __BUILD_DOCUMENT_PROCESSOR_H

#include "build_service/processor/DocumentProcessor.h"
#include <tr1/memory>

namespace build_service {
namespace processor {

class SimpleDocumentProcessor : public DocumentProcessor
{
public:
    SimpleDocumentProcessor();
    ~SimpleDocumentProcessor();
public:
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    /* override */ void destroy();
    /* override */ DocumentProcessor* clone() {return new SimpleDocumentProcessor(*this);}
    /* override */ bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader);

    /* override */ std::string getDocumentProcessorName() const {return "SimpleDocumentProcessor";}

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;
    analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
};

typedef std::tr1::shared_ptr<SimpleDocumentProcessor> SimpleDocumentProcessorPtr;

}
}

#endif //__BUILD_DOCUMENT_PROCESSOR_H
