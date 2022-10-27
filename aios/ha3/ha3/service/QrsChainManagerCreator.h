#ifndef ISEARCH_QRSCHAINMANAGERCREATOR_H
#define ISEARCH_QRSCHAINMANAGERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/qrs/QrsChainManager.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/plugin/SorterModuleFactory.h>
#include <ha3/turing/qrs/QrsBiz.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

BEGIN_HA3_NAMESPACE(service);

class QrsChainManagerCreator
{
public:
    QrsChainManagerCreator();
    ~QrsChainManagerCreator();
public:
    static qrs::QrsChainManagerPtr createQrsChainMgr(turing::QrsBiz *qrsBiz);
private:
    static build_service::analyzer::AnalyzerFactoryPtr createAnalyzerFactory(
            const config::ResourceReaderPtr &resourceReader);
    static bool createSorterManager(config::ResourceReaderPtr &resourceReader,
                                    const config::ConfigAdapterPtr &configAdapterPtr,
                                    suez::turing::ClusterSorterManagersPtr &clusterSorterManagersPtr,
                                    const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
                                    kmonitor::MetricsReporter *metricsReporter);
    static suez::turing::SorterManagerPtr doCreateSorterManager(const std::string &clusterName,
                                                                config::ResourceReaderPtr &resourceReader,
                                                                const config::ConfigAdapterPtr &configAdapterPtr,
                                                                kmonitor::MetricsReporter *metricsReporter);

    static void getClusterSorterNames(
            const suez::turing::ClusterSorterManagersPtr &clusterSorterManangersPtr,
            suez::turing::ClusterSorterNamesPtr &clusterSorterNamesPtr);
    static suez::turing::ClusterFuncMapPtr createClusterFuncMap(
            const std::map<std::string, suez::turing::FuncConfig> &funcConfigMap,
            config::ResourceReaderPtr &resourceReader,
            suez::turing::CavaJitWrapper *cavaJitWrapper);
    static suez::turing::CavaPluginManagerMapPtr createCavaPluginManagerMap(
            const std::map<std::string, suez::turing::CavaConfig> &cavaConfigMap,
            const std::map<std::string, suez::turing::FuncConfig> &funcConfigMap,
            config::ResourceReaderPtr &resourceReader,
            kmonitor::MetricsReporter *metricsReporter);
private:
    friend class QrsChainManagerCreatorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsChainManagerCreator);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSCHAINMANAGERCREATOR_H
