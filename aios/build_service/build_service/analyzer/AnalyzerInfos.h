#ifndef ISEARCH_BS_ANALYZER_SCHEMA_H
#define ISEARCH_BS_ANALYZER_SCHEMA_H

#include "build_service/util/Log.h"
#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "build_service/analyzer/TokenizerConfig.h"
#include "build_service/analyzer/AnalyzerInfo.h"
#include "build_service/analyzer/TraditionalTables.h"
#include <map>

namespace build_service {
namespace analyzer {

class AnalyzerInfos : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, AnalyzerInfo> AnalyzerInfoMap;
    typedef AnalyzerInfoMap::const_iterator ConstIterator;
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool validate() const;

public:
    void addAnalyzerInfo(const std::string &name,
                         const AnalyzerInfo &analyzerInfo);
    const AnalyzerInfo* getAnalyzerInfo(const std::string& analyzerName) const;

    size_t getAnalyzerCount() const { return _analyzerInfos.size(); }

    const TraditionalTables &getTraditionalTables() const {
        return _traditionalTables;
    }

    void setTraditionalTables(const TraditionalTables &tables) {
        _traditionalTables = tables;
    }
    const TokenizerConfig& getTokenizerConfig() const {
        return _tokenizerConfig;
    }
    ConstIterator begin() const { return _analyzerInfos.begin();}
    ConstIterator end() const { return _analyzerInfos.end();}

    void merge(const AnalyzerInfos& analyzerInfos);

    void makeCompatibleWithOldConfig();

private:
    AnalyzerInfoMap _analyzerInfos;
    TokenizerConfig _tokenizerConfig;
    TraditionalTables _traditionalTables;
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<AnalyzerInfos> AnalyzerInfosPtr;

}
}

#endif //ISEARCH_BS_ANALYZER_SCHEMA_H
