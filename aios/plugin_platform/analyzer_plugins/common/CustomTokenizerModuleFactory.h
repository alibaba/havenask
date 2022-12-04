#pragma once

#include <string>

#include "build_service/analyzer/TokenizerModuleFactory.h"
#include "build_service/config/ResourceReader.h"
#include "util/Log.h"

namespace build_service {
namespace analyzer {
class Tokenizer;
}  // namespace analyzer
}  // namespace build_service

namespace pluginplatform {
namespace analyzer_plugins {

class CustomTokenizerModuleFactory : public build_service::analyzer::TokenizerModuleFactory
{
    typedef CustomTokenizerModuleFactory _Super;
public:
    CustomTokenizerModuleFactory() {}
    virtual ~CustomTokenizerModuleFactory() {}

    /*override*/ virtual bool init(const build_service::KeyValueMap& parameters , const build_service::config::ResourceReaderPtr& resourceReader){
        return true;
    }

    /*override*/ virtual void destroy() { delete this; }

    /*override tokenizer*/
    virtual build_service::analyzer::Tokenizer* createTokenizer(const std::string& tokenizerType);

private:
    CustomTokenizerModuleFactory(const CustomTokenizerModuleFactory &);
    CustomTokenizerModuleFactory& operator=(const CustomTokenizerModuleFactory &);

private:
    PLUG_LOG_DECLARE();
};

}}
