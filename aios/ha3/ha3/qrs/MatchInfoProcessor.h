#ifndef ISEARCH_MATCHINFOPROCESSOR_H
#define ISEARCH_MATCHINFOPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <vector>
#include <string>
#include <ha3/qrs/QrsProcessor.h>
#include <map>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/qrs/CheckMatchVisitor.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(qrs);
class CheckResult;
class MatchInfoProcessor : public QrsProcessor
{
public:
    MatchInfoProcessor(const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr)
        : _clusterTableInfoMapPtr(clusterTableInfoMapPtr)
    {
    }

    virtual ~MatchInfoProcessor(){}
public:
    
    std::string getName() const override {
        return DEFAULT_DEBUG_PROCESSOR;
    }
    bool init(const KeyValueMap &keyValues, 
              config::ResourceReader *resourceReader) override;
    void process(common::RequestPtr &requestPtr,
                 common::ResultPtr &resultPtr) override;
    void fillSummary(const common::RequestPtr &requestPtr,
                     const common::ResultPtr &resultPtr) override;
    QrsProcessorPtr clone() override;
    
    void setAnalyzerFactory(const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactoryPtr) {
        _analyzerFactoryPtr = analyzerFactoryPtr;
    }

private:
    bool rewriteQuery(common::RequestPtr &requestPtr);
    void analyzeMatchInfo(const common::RequestPtr &requestPtr, 
                          const common::ResultPtr &resultPtr);
    void getQueryIndexs(const suez::turing::TableInfoPtr& tableInfoPtr, 
                        std::set<std::string>& queryIndexs);

    void reportMatchResult(const CheckResult& checkResult);
    void parseMatchInfo(const common::HitPtr& hitPtr, 
                        std::map<std::string, bool>& matchTermInfo);
    void traceQuery(const std::string& prefix, const common::Query *query);

    void getSummaryInfo(const suez::turing::TableInfoPtr& tableInfoPtr, 
                        std::map<std::string, std::string>& summaryInfo);
    std::map<std::string, std::string> getSummaryFields(const common::HitPtr& hitPtr,
            const std::map<std::string, std::string>& summaryInfo);
    void tokenSummary(std::map<std::string, std::string>& summaryMap,
                      const std::map<std::string, std::string>& summaryInfo);

    std::string getAnalyzerName(const std::string& fieldName, 
                                const std::map<std::string, std::string>& summaryInfo);
    void getAttributes(std::map<std::string, std::string>& attributeMap,
                       const common::HitPtr& hitPtr);
    void checkPhraseError(const std::map<std::string, std::string>& phraseMap, 
                          const std::map<std::string, std::string>& summaryMap, 
                          CheckResultInfo& resInfo);
    void removeSummaryTag(std::string& titleStr);
    void replaceAll(std::string& str, const std::string& from, 
                    const std::string& to); 
    suez::turing::TableInfoPtr getTableInfo(const std::string& clusterName) const;
private:
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr; //for tokenize summary info.
    std::set<std::string> _queryIndexs;
    std::map<std::string, std::string> _summaryFields;
private:
    friend class MatchInfoProcessorTest;
    HA3_LOG_DECLARE();
};

class CheckQueryResult : public autil::legacy::Jsonizable{
public:
    std::string _queryStr;
    std::string _errorSubQueryStr;
    std::string _errorType;
    std::string _errorWord;
    bool _isTokenizePart;
    std::map<std::string, std::string> _summaryMap;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

class CheckFilterResult : public autil::legacy::Jsonizable{
public:
    std::map<std::string, std::string> _filterValMap;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

class CheckResult : public autil::legacy::Jsonizable{
public:
    CheckQueryResult _checkQueryResult;
    CheckFilterResult _checkFilterResult;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

END_HA3_NAMESPACE(qrs);
#endif 
