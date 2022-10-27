#ifndef ISEARCH_BS_TOKENIZERMODULEFACTORY_H
#define ISEARCH_BS_TOKENIZERMODULEFACTORY_H

#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace analyzer {

class Tokenizer;

class TokenizerModuleFactory : public plugin::ModuleFactory
{
public:
    TokenizerModuleFactory() {}
    virtual ~TokenizerModuleFactory() {}
private:
    TokenizerModuleFactory(const TokenizerModuleFactory &);
    TokenizerModuleFactory& operator=(const TokenizerModuleFactory &);
public:
    virtual bool init(const KeyValueMap &parameters,
                      const config::ResourceReaderPtr &resourceReader) = 0;
    virtual void destroy() = 0;
    virtual Tokenizer* createTokenizer(const std::string &tokenizerType) = 0;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizerModuleFactory);
}
}

#endif //ISEARCH_BS_TOKENIZERMODULEFACTORY_H
