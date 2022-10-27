#ifndef ISEARCH_QRSCHAINMANAGER_H
#define ISEARCH_QRSCHAINMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/qrs/QrsChainConfig.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <ha3/config/QrsConfig.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <suez/turing/expression/function/FunctionMap.h>
#include <kmonitor/client/MetricsReporter.h>
#include <map>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

BEGIN_HA3_NAMESPACE(qrs);

/** the class to manage processor chain in QRS service. */
class QrsChainManager
{
public:
    QrsChainManager(build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
                    config::ResourceReaderPtr &resourceReaderPtr);
    ~QrsChainManager();
public:

    //construct 'QrsChain' list
    bool init();

    //get 'Processor' chain according chain's name
    QrsProcessorPtr getProcessorChain(const std::string &chainName);
    void setQrsChainConfig(const QrsChainConfig &qrsChainConfig);
    void setPlugInManagerPtr(build_service::plugin::PlugInManagerPtr &plugInManagerPtr);
    void setClusterTableInfoMap(const suez::turing::ClusterTableInfoMapPtr &tableInfoMapPtr);
    void setClusterConfigMap(const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    void setAnalyzerFactory(const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactoryPtr);
    void setFuncInfoMap(const suez::turing::ClusterFuncMapPtr &clusterFuncMapPtr);
    void setCavaPluginManagerMap(const suez::turing::CavaPluginManagerMapPtr &clusterCavaPluginManagerMapPtr);
    void setClusterSorterManagers(const suez::turing::ClusterSorterManagersPtr &clusterSorterManagersPtr);
    void setClusterSorterNames(const suez::turing::ClusterSorterNamesPtr &clusterSorterNamesPtr);
    void setQRSRule(const config::QRSRule &qrsRule);
    const config::QRSRule& getQRSRule();
    void setRequestCompressType(HaCompressType type) {
        _requestCompressType = type;
    }
    const HaCompressType& getRequestCompressType() const {
        return _requestCompressType;
    }

    void setMetricsReporter(kmonitor::MetricsReporter *metricsReporter);
    suez::turing::ClusterSorterManagersPtr getClusterSorterManagers() const {
        return _clusterSorterManagersPtr;
    }

public:
    //interface for test
    void addProcessorChain(const std::string &chainName,
                           QrsProcessorPtr chain);
    void addProcessorInfo(const config::ProcessorInfo &processorInfo);
    void addQrsChainInfo(const config::QrsChainInfo &chainInfo);
    config::ProcessorInfo &getProcessorInfo(const std::string &processorName);

    void clear();
private:
    bool constructChain(const config::QrsChainInfo &chainInfo);

    QrsProcessorPtr createValidateProcessor();

    bool createProcessorsBeforeParserPoint(const config::QrsChainInfo &chainInfo,
            QrsProcessorPtr &chainHead,
            QrsProcessorPtr &chainTail);
    bool createProcessorsBeforeValidatePoint(const config::QrsChainInfo &chainInfo,
            QrsProcessorPtr &chainHead,
            QrsProcessorPtr &chainTail);
    bool createProcessorsBeforeSearchPoint(const config::QrsChainInfo &chainInfo,
            QrsProcessorPtr &chainHead,
            QrsProcessorPtr &chainTail);
    bool createProcessors(const config::ProcessorNameVec &processorNames,
                          QrsProcessorPtr &chainHead,
                          QrsProcessorPtr &chainTail);
    void linkChain(QrsProcessorPtr newProcessorPtr,
                   QrsProcessorPtr &chainHead,
                   QrsProcessorPtr &chainTail);
    QrsProcessorPtr cloneProcessorChain(QrsProcessorPtr firstProcessor);
    QrsProcessorPtr createBuildInProcessor(const std::string &processorName);
private:
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    std::map<std::string, QrsProcessorPtr> _chainMap;
    QrsChainConfig _chainConfig;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;

    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    suez::turing::ClusterFuncMapPtr _clusterFuncMapPtr;
    suez::turing::CavaPluginManagerMapPtr _clusterCavaPluginManagerMapPtr;
    config::QRSRule _qrsRule;
    HaCompressType _requestCompressType;
    config::ResourceReaderPtr _resourceReaderPtr;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
    suez::turing::ClusterSorterNamesPtr _clusterSorterNamesPtr;
    kmonitor::MetricsReporter *_metricsReporter;
private:
    friend class QrsChainManagerTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsChainManager);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSCHAINMANAGER_H
