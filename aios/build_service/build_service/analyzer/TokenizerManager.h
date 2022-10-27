#ifndef ISEARCH_BS_TOKENIZERMANAGER_H
#define ISEARCH_BS_TOKENIZERMANAGER_H

#include "build_service/util/Log.h"
#include "build_service/analyzer/TokenizerConfig.h"

namespace build_service {
namespace analyzer {
class Tokenizer;
class TokenizerModuleFactory;
class BuildInTokenizerFactory;
}
}

namespace build_service {
namespace plugin {
class PlugInManager;
}
}

namespace build_service {
namespace config {
class ResourceReader;
}
}

namespace build_service {
namespace analyzer {

class TokenizerManager
{
public:
    TokenizerManager(const std::tr1::shared_ptr<config::ResourceReader> &resourceReaderPtr);
    ~TokenizerManager();
private:
    TokenizerManager(const TokenizerManager &);
    TokenizerManager& operator=(const TokenizerManager &);
public:
    bool init(const TokenizerConfig &tokenizerConfig);
    Tokenizer* getTokenizerByName(const std::string &name) const;

public:
    //for test
    void setBuildInFactory(const std::tr1::shared_ptr<BuildInTokenizerFactory> &buildInFactoryPtr) {
        _buildInFactoryPtr = buildInFactoryPtr;
    }
    void addTokenizer(const std::string &tokenizerName, Tokenizer *tokenizer);

private:
    bool exist(const std::string &tokenizerName) const;
    TokenizerModuleFactory* getModuleFactory(const std::string &moduleName);
    const std::tr1::shared_ptr<plugin::PlugInManager>& getPluginManager();
private:
    std::map<std::string, Tokenizer*> _tokenizerMap;
    std::tr1::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    std::tr1::shared_ptr<plugin::PlugInManager> _plugInManagerPtr;
    std::tr1::shared_ptr<BuildInTokenizerFactory> _buildInFactoryPtr;
    TokenizerConfig _tokenizerConfig;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizerManager);

}
}

#endif //ISEARCH_BS_TOKENIZERMANAGER_H
