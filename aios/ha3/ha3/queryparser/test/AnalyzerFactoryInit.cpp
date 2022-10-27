#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <build_service/analyzer/SimpleTokenizer.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <build_service/analyzer/AnalyzerDefine.h>
#include <ha3/test/test.h>
using namespace std;

using namespace build_service::config;
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, AnalyzerFactoryInit);


void AnalyzerFactoryInit::initFactory(AnalyzerFactory &factory) {
    AnalyzerInfo simpleInfo;
    simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    simpleInfo.setTokenizerConfig(DELIMITER, " ");
    simpleInfo.setTokenizerName("simple_tokenizer");
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    infosPtr->addAnalyzerInfo("alimama", simpleInfo);
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(ResourceReaderPtr()));
    Tokenizer * tokenizer = new SimpleTokenizer(" ");
    tokenizerManagerPtr->addTokenizer("simple_tokenizer", tokenizer);
    factory.init(infosPtr, tokenizerManagerPtr);
}

END_HA3_NAMESPACE(queryparser);

