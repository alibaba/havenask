#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/Analyzer.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/TokenizerManager.h"
#include "build_service/analyzer/SimpleTokenizer.h"
#include "build_service/analyzer/SingleWSTokenizer.h"
#include "build_service/analyzer/IdleTokenizer.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

using namespace build_service::util;
using namespace build_service::config;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, AnalyzerFactory);

AnalyzerFactory::AnalyzerFactory() {
}

AnalyzerFactory::~AnalyzerFactory() {
    for (map<string, Analyzer*>::iterator it = _analyzerMap.begin();
         it != _analyzerMap.end(); ++it) {
        DELETE_AND_SET_NULL(it->second);
    }
}

AnalyzerFactoryPtr AnalyzerFactory::create(const ResourceReaderPtr &resourceReaderPtr) {
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos);
    if (!resourceReaderPtr->getConfigWithJsonPath(ANALYZER_CONFIG_FILE, "", *infosPtr)) {
        string errorMsg = "Init Analyzer info failed, check [" + ANALYZER_CONFIG_FILE + "] file.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return AnalyzerFactoryPtr();
    }
    infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr managerPtr(
            new TokenizerManager(resourceReaderPtr));
    if (!managerPtr->init(infosPtr->getTokenizerConfig())) {
        string errorMsg = "Failed to create TokenizerManagerPtr";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return AnalyzerFactoryPtr();
    }
    AnalyzerFactoryPtr factory(new AnalyzerFactory);
    if (!factory->init(infosPtr, managerPtr)) {
        string errorMsg = "Failed to init AnalyzerFactory.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return AnalyzerFactoryPtr();
    }
    return factory;
}

bool AnalyzerFactory::init(const AnalyzerInfosPtr analyzerInfosPtr,
                           const TokenizerManagerPtr &tokenizerManager)
{
    if (!tokenizerManager) {
        string errorMsg = "The TokenizerManagerPtr is NULL!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _tokenizerManagerPtr = tokenizerManager;
    if (0 == analyzerInfosPtr->getAnalyzerCount()) {
        return true;
    }

    const TraditionalTables &traditionalTables = analyzerInfosPtr->getTraditionalTables();
    for(AnalyzerInfos::ConstIterator it = analyzerInfosPtr->begin();
        it != analyzerInfosPtr->end(); ++it) {
        const AnalyzerInfo &analyzerInfo = it->second;
        const string &tokenizerName = analyzerInfo.getTokenizerName();
        Tokenizer* tokenizer = _tokenizerManagerPtr->getTokenizerByName(tokenizerName);
        if (!tokenizer) {
            string errorMsg = "create tokenizer [" + tokenizerName+ "] failed!";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        Analyzer* analyzer = new Analyzer(tokenizer);
        const string& traditionalTableName = analyzerInfo.getNormalizeOptions().tableName;
        const TraditionalTable *traditionalTable = NULL;
        if (!traditionalTableName.empty()) {
            traditionalTable = traditionalTables.getTraditionalTable(traditionalTableName);
            if (!traditionalTable) {
                DELETE_AND_SET_NULL(analyzer);
                string errorMsg = "can not find traditional table ["
                                  + traditionalTableName + "]!";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        }
        analyzer->init(&(it->second),traditionalTable);
        _analyzerMap[it->first] = analyzer;
        BS_LOG(TRACE3, "add analyzer [%s] success", it->first.c_str());
    }
    BS_LOG(TRACE3, "map size: %zd", _analyzerMap.size());
    return true;
}

Analyzer* AnalyzerFactory::createAnalyzer(const string &name,
        bool needIdleTokenizer) const
{
    map<string, Analyzer*>::const_iterator it = _analyzerMap.find(name);
    if (it == _analyzerMap.end()) {
        string errorMsg = "Cannot find analyzer with name [" + name + "] in anayler map.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    Analyzer* analyzer = it->second->clone();
    if (needIdleTokenizer) {
        analyzer->resetTokenizer(new IdleTokenizer());
    }
    return analyzer;
}

bool AnalyzerFactory::hasAnalyzer(const string &name) const {
    return _analyzerMap.find(name) != _analyzerMap.end();
}

}
}
