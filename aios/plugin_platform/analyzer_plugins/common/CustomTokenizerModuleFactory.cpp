
#include "common/CustomTokenizerModuleFactory.h"
#include "jieba/JiebaTokenizer.h"


using pluginplatform::analyzer_plugins::JiebaTokenizer;
using namespace build_service::analyzer;

namespace pluginplatform {
namespace analyzer_plugins {

PLUG_LOG_SETUP(plug, CustomTokenizerModuleFactory);

Tokenizer* CustomTokenizerModuleFactory::createTokenizer(const std::string& tokenizerType)
{
    if ( tokenizerType == "jieba_analyzer" ){
        return new JiebaTokenizer;
    }
    PLUG_LOG(ERROR,"Tokenizer type %s Not Found.",tokenizerType.c_str());
    return 0;
}
}}

extern "C" build_service::plugin::ModuleFactory* createFactory_Tokenizer()
{
    return (build_service::plugin::ModuleFactory*) new (std::nothrow) pluginplatform::analyzer_plugins::CustomTokenizerModuleFactory();
}

extern "C" void destroyFactory_Tokenizer(build_service::plugin::ModuleFactory *factory)
{
    factory->destroy();
}
