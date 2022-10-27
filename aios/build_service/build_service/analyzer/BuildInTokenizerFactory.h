#ifndef ISEARCH_BS_BUILDINTOKENIZERFACTORY_H
#define ISEARCH_BS_BUILDINTOKENIZERFACTORY_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/TokenizerModuleFactory.h"

namespace build_service {
namespace analyzer {

class Tokenizer;

class BuildInTokenizerFactory : public TokenizerModuleFactory
{
public:
    BuildInTokenizerFactory();
    virtual ~BuildInTokenizerFactory();
private:
    BuildInTokenizerFactory(const BuildInTokenizerFactory &);
    BuildInTokenizerFactory& operator=(const BuildInTokenizerFactory &);
public:
    virtual bool init(const KeyValueMap &parameters,
                      const config::ResourceReaderPtr &resourceReader);
    virtual void destroy();
    virtual Tokenizer* createTokenizer(const std::string &tokenizerType);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildInTokenizerFactory);

}
}

#endif //ISEARCH_BS_BUILDINTOKENIZERFACTORY_H
