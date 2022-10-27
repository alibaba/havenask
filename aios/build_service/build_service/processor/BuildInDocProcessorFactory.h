#ifndef ISEARCH_BS_BUILDINDOCPROCESSORFACTORY_H
#define ISEARCH_BS_BUILDINDOCPROCESSORFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessorFactory.h"
namespace build_service {
namespace processor {

class BuildInDocProcessorFactory : public DocumentProcessorFactory
{
public:
    BuildInDocProcessorFactory();
    ~BuildInDocProcessorFactory();
private:
    BuildInDocProcessorFactory(const BuildInDocProcessorFactory &);
    BuildInDocProcessorFactory& operator=(const BuildInDocProcessorFactory &);
public:
    /* override */ bool init(const KeyValueMap &parameters) {
        return true;
    }
    /* override */ void destroy() {
        delete this;
    }
    /* override */ DocumentProcessor* createDocumentProcessor(const std::string &processorName);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildInDocProcessorFactory);

}
}

#endif //ISEARCH_BS_BUILDINDOCPROCESSORFACTORY_H
