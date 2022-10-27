#ifndef ISEARCH_QRSCHAINMANAGERCONFIGURATOR_H
#define ISEARCH_QRSCHAINMANAGERCONFIGURATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsChainManager.h>
#include <ha3/service/QrsSearcherProcessDelegation.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <kmonitor/client/MetricsReporter.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
BEGIN_HA3_NAMESPACE(qrs);

class QrsChainManagerConfigurator
{
public:
    QrsChainManagerConfigurator(
            const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
            const config::ClusterConfigMapPtr &clusterConfigMapPtr,
            const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactoryPtr,
            const suez::turing::ClusterFuncMapPtr &clusterFuncMapPtr,
            const suez::turing::CavaPluginManagerMapPtr &clusterCavaPluginManagerMapPtr,
            const config::ResourceReaderPtr &resourceReaderPtr,
            const suez::turing::ClusterSorterManagersPtr &clusterSorterManagersPtr,
            const suez::turing::ClusterSorterNamesPtr &clusterSorterNamesPtr)
    {
        _clusterTableInfoMapPtr = clusterTableInfoMapPtr;
        _clusterConfigMapPtr = clusterConfigMapPtr;
        _analyzerFactoryPtr = analyzerFactoryPtr;
        _clusterFuncMapPtr = clusterFuncMapPtr;
        _clusterCavaPluginManagerMapPtr = clusterCavaPluginManagerMapPtr;
        _resourceReaderPtr = resourceReaderPtr;
        _clusterSorterManagersPtr = clusterSorterManagersPtr;
        _clusterSorterNamesPtr = clusterSorterNamesPtr;
        _metricsReporter = NULL;
    }
    ~QrsChainManagerConfigurator() {}

public:
    QrsChainManagerPtr createFromFile(const std::string &configPath);
    QrsChainManagerPtr createFromString(const std::string &content);
    QrsChainManagerPtr create(config::QrsConfig &qrsConfig);
    void setMetricsReporter(kmonitor::MetricsReporter *metricsReporter);
private:
    void addDebugQrsChain(config::QrsConfig& qrsConfig);
private:
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
    suez::turing::ClusterFuncMapPtr _clusterFuncMapPtr;
    suez::turing::CavaPluginManagerMapPtr _clusterCavaPluginManagerMapPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
    suez::turing::ClusterSorterNamesPtr _clusterSorterNamesPtr;
    kmonitor::MetricsReporter *_metricsReporter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsChainManagerConfigurator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSCHAINMANAGERCONFIGURATOR_H
