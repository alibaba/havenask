#ifndef ISEARCH_QRSSEARCHPROCESSOR_H
#define ISEARCH_QRSSEARCHPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/plugin/SorterManager.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsSearchProcessor : public QrsProcessor
{
public:
    QrsSearchProcessor(
            int32_t connectionTimeout,
            const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
            const config::ClusterConfigMapPtr &clusterConfigMapPtr,
            const suez::turing::ClusterSorterManagersPtr &clusterSorterManagersPtr,
            HaCompressType requestCompressType,
            const config::SearchTaskQueueRule &searchTaskqueueRule);
    ~QrsSearchProcessor();
private:
    QrsSearchProcessor(const QrsSearchProcessor& processor);
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
    virtual void fillSummary(const common::RequestPtr &requestPtr,
                             const common::ResultPtr &resultPtr);
    std::string getName() const { return "QrsSearchProcessor"; }
    // for test
    const service::QrsSearcherProcessDelegation* getSearcherDelegation() const {
        return _qrsSearcherProcessDelegation;
    }
private:
    void initSearcherDelegation();
    void initQrsSearchProcesser();
    common::ResultPtr convertFetchSummaryClauseToHits(
            const common::RequestPtr &requestPtr);
    uint32_t calculateHashId(const std::string &clusterName,
                             const std::string &rawPk);
    primarykey_t calculatePrimaryKey(const std::string &clusterName,
            const std::string &rawPk);
    static void fillSearcherCacheKey(common::RequestPtr &requestPtr);
    void convertSummaryCluster(common::Hits *hits, common::ConfigClause *configClause);
    void fillPositionForHits(const common::ResultPtr &result);
    void rewriteMetricSrc(common::ConfigClause *configClause);
private:
    // for test
    void setSearcherDelegation(service::QrsSearcherProcessDelegation* delegation);
    void flatten(common::QueryClause *queryClause);
private:
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    service::HitSummarySchemaCachePtr _hitSummarySchemaCache;
    service::QrsSearcherProcessDelegation *_qrsSearcherProcessDelegation;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
private:
    friend class QrsSearchProcessorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsSearchProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSSEARCHPROCESSOR_H
