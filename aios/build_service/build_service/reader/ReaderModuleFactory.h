#ifndef ISEARCH_BS_READERMODULEFACTORY_H
#define ISEARCH_BS_READERMODULEFACTORY_H

#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace reader {

class RawDocumentReader;

class ReaderModuleFactory : public plugin::ModuleFactory
{
public:
    ReaderModuleFactory() {}
    virtual ~ReaderModuleFactory() {}
private:
    ReaderModuleFactory(const ReaderModuleFactory &);
    ReaderModuleFactory& operator=(const ReaderModuleFactory &);
public:
    virtual bool init(const KeyValueMap &parameters) = 0;
    virtual void destroy() = 0;
    virtual RawDocumentReader* createRawDocumentReader(const std::string &readerName) = 0;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReaderModuleFactory);
}
}

#endif //ISEARCH_BS_TOKENIZERMODULEFACTORY_H
