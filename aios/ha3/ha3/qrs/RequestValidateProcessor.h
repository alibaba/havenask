#ifndef ISEARCH_REQUESTVALIDATEPROCESSOR_H
#define ISEARCH_REQUESTVALIDATEPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/qrs/RequestValidator.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/util/TableInfo.h>

BEGIN_HA3_NAMESPACE(qrs);

class RequestValidateProcessor : public QrsProcessor
{
public:
    RequestValidateProcessor(
            const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
            const config::ClusterConfigMapPtr &clusterConfigMapPtr,
            RequestValidatorPtr &requestValidatorPtr,
            QueryTokenizer &queryTokenizer);
    virtual ~RequestValidateProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "RequestValidateProcessor";
    }
public:
    static void fillPhaseOneInfoMask(common::ConfigClause *configClause,
            const suez::turing::IndexInfo *summaryPrimaryKeyIndex);
private:
    bool validateRequest(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    bool validateMultiCluster(const common::ConfigClause *configClause,
                              const common::LevelClause *levelClause,
                              common::MultiErrorResultPtr &errorResultPtr);
    bool validatePbMatchDocFormat(common::ConfigClause *configClause,
                                  common::MultiErrorResultPtr &errorResultPtr);
    bool validateFetchSummaryCluster(common::ConfigClause *configClause, 
            common::MultiErrorResultPtr &errorResultPtr);
    
    bool getTableInfo(const std::string& clusterName,
                      common::MultiErrorResultPtr &errorResultPtr,
                      suez::turing::TableInfoPtr &tableInfoPtr);
    bool schemaCompatible(const suez::turing::TableInfoPtr &tableInfoPtr1,
                          const suez::turing::TableInfoPtr &tableInfoPtr2);
    bool indexInfoCompatible(const suez::turing::IndexInfos* indexInfo1,
                             const suez::turing::IndexInfos* indexInfo2);
    bool attributeInfoCompatible(const suez::turing::AttributeInfos* attrInfo1,
                                 const suez::turing::AttributeInfos* attrInfo2);
    void setClusterTableInfoMapPtr(const suez::turing::ClusterTableInfoMapPtr &tableInfoMap);
    void setClusterConfigInfoMap(const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    bool getClusterConfigInfo(const std::string &clusterName,
                              config::ClusterConfigInfo& clusterConfig) const;
    bool tokenizeAndCleanStopWord(
            common::Query *rootQuery,
            QueryOperator defaultOP,
            const suez::turing::IndexInfos *indexInfos,
            const common::MultiErrorResultPtr &errorResultPtr,
            common::Query **ppCleanQuery);
    //getAuxTableInfo();
    bool getAuxTableInfo(
            const std::string &clusterName,
            const common::MultiErrorResultPtr &errorResultPtr, 
            suez::turing::TableInfoPtr &auxTableInfoPtr);
    static std::string getBizName(const std::string &name);
private:
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    QueryTokenizer _queryTokenizer;
    RequestValidator _requestValidator;

    friend class RequestValidateProcessorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RequestValidateProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_REQUESTVALIDATEPROCESSOR_H
