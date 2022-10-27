#ifndef ISEARCH_BS_ANALYZERFACTORY_H
#define ISEARCH_BS_ANALYZERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"

namespace build_service {
namespace analyzer {

class Analyzer;
class AnalyzerInfos;
BS_TYPEDEF_PTR(AnalyzerInfos);
class TokenizerManager;
BS_TYPEDEF_PTR(TokenizerManager);
class AnalyzerFactory;
BS_TYPEDEF_PTR(AnalyzerFactory);

class AnalyzerFactory
{
public:
    AnalyzerFactory();
    virtual ~AnalyzerFactory();
public:
    static AnalyzerFactoryPtr create(const config::ResourceReaderPtr &resourceReaderPtr);
public:
    Analyzer* createAnalyzer(const std::string &name,
                             bool needIdleTokenizer = false) const;
    bool hasAnalyzer(const std::string &name) const;
public: // for test
    bool init(const AnalyzerInfosPtr analyzerInfosPtr,
              const TokenizerManagerPtr &tokenizerManagerPtr);
private:
    std::map<std::string, Analyzer*> _analyzerMap;
    TokenizerManagerPtr _tokenizerManagerPtr;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_ANALYZERFACTORY_H
