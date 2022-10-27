#include <ha3/service/QrsChainManagerCreator.h>
#include <ha3/qrs/QrsChainManagerConfigurator.h>
#include <ha3/config/ResourceReader.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace kmonitor;

USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(turing);

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsChainManagerCreator);


QrsChainManagerCreator::QrsChainManagerCreator() {
}

QrsChainManagerCreator::~QrsChainManagerCreator() {
}

QrsChainManagerPtr QrsChainManagerCreator::createQrsChainMgr(QrsBiz *qrsBiz)
{
    const ConfigAdapterPtr &configAdapterPtr = qrsBiz->getConfigAdapter();
    string bizName = qrsBiz->getBizName();
    HA3_LOG(DEBUG, "biz [%s] Begin CreateQrsChainManager !!", bizName.c_str());
    if (!configAdapterPtr) {
        HA3_LOG(ERROR,"error, configAdapter pointer is NULL!!!");
        return QrsChainManagerPtr();
    }

    ClusterConfigMapPtr clusterConfigMapPtr = configAdapterPtr->getClusterConfigMap();
    if (!clusterConfigMapPtr) {
        HA3_LOG(ERROR, "Get section [cluster_config] failed!");
        return QrsChainManagerPtr();
    }

    ClusterTableInfoMapPtr clusterTableInfoMapPtr =
        configAdapterPtr->getClusterTableInfoMap();
    if (!clusterTableInfoMapPtr) {
        HA3_LOG(ERROR, "get cluster table info map failed");
        return QrsChainManagerPtr();
    }
    ResourceReaderPtr resourceReaderPtr(
            new ResourceReader(configAdapterPtr->getConfigPath()));
    AnalyzerFactoryPtr analyzerFactoryPtr = createAnalyzerFactory(resourceReaderPtr);
    if (!analyzerFactoryPtr) {
        HA3_LOG(ERROR, "Create analyzer factory failed!");
        return QrsChainManagerPtr();
    }

    map<string, FuncConfig> funcConfigMap;
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); ++it)
    {
        if (StringUtil::endsWith(it->first, HA3_DEFAULT_AGG)) {
            continue;
        }
        if (!configAdapterPtr->getFuncConfig(it->first, funcConfigMap[it->first])) {
            HA3_LOG(ERROR, "Get section [function_config] for cluster[%s] failed.", it->first.c_str());
            return QrsChainManagerPtr();
        }
    }
    // get func config for default agg
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); ++it)
    {
        if (StringUtil::endsWith(it->first, HA3_DEFAULT_AGG)) {
            string defaultBiz = it->first.substr(0, it->first.size() - HA3_DEFAULT_AGG.size()) +
                                bizName;
            auto iter = funcConfigMap.find(defaultBiz);
            if (iter != funcConfigMap.end()) {
                funcConfigMap[it->first] = iter->second;
            }
        }
    }

    map<string, CavaConfig> cavaConfigMap;
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); ++it)
    {
        if (StringUtil::endsWith(it->first, HA3_DEFAULT_AGG)) {
            continue;
        }
        if (!configAdapterPtr->getCavaConfig(it->first, cavaConfigMap[it->first])) {
            HA3_LOG(ERROR, "Get section [cava_config] for cluster[%s] failed.", it->first.c_str());
            return QrsChainManagerPtr();
        }
    }

    // get cava config for default agg
    for (ClusterConfigMap::const_iterator it = clusterConfigMapPtr->begin();
         it != clusterConfigMapPtr->end(); ++it)
    {
        if (StringUtil::endsWith(it->first, HA3_DEFAULT_AGG)) {
            string defaultBiz = it->first.substr(0, it->first.size() - HA3_DEFAULT_AGG.size()) +
                                bizName;
            auto iter = cavaConfigMap.find(defaultBiz);
            if (iter != cavaConfigMap.end()) {
                cavaConfigMap[it->first] = iter->second;
            }
        }
    }

    QrsConfig qrsConfig;
    if (!configAdapterPtr->getQrsConfig(qrsConfig)) {
        HA3_LOG(ERROR, "get QrsConfig failed");
        return QrsChainManagerPtr();
    }

    kmonitor::MetricsReporter *pluginMetricsReporter = qrsBiz->getPluginMetricsReporter();
    auto cavaJitWrapper = qrsBiz->getCavaJitWrapper();
    ClusterSorterManagersPtr clusterSorterManangersPtr(
            new ClusterSorterManagers);
    if (!createSorterManager(resourceReaderPtr, configAdapterPtr,
                             clusterSorterManangersPtr, clusterTableInfoMapPtr,
                             pluginMetricsReporter))
    {
        HA3_LOG(ERROR, "generate sorter manangers failed");
        return QrsChainManagerPtr();
    }
    ClusterSorterNamesPtr clusterSorterNamesPtr(new ClusterSorterNames());
    getClusterSorterNames(clusterSorterManangersPtr, clusterSorterNamesPtr);

    ClusterFuncMapPtr clusterFuncMapPtr = createClusterFuncMap(funcConfigMap,
            resourceReaderPtr, cavaJitWrapper);
    if (!clusterFuncMapPtr) {
        HA3_LOG(ERROR, "get function info map failed.");
        return QrsChainManagerPtr();
    }

    CavaPluginManagerMapPtr clusterCavaPluginManagerMapPtr = createCavaPluginManagerMap(cavaConfigMap,
            funcConfigMap, resourceReaderPtr, pluginMetricsReporter);
    if (!clusterCavaPluginManagerMapPtr) {
        HA3_LOG(ERROR, "create cavaPluginManagerMap map failed.");
        return QrsChainManagerPtr();
    }

    QrsChainManagerConfigurator qrsChainManagerConfig(
            clusterTableInfoMapPtr, clusterConfigMapPtr,
            analyzerFactoryPtr, clusterFuncMapPtr, clusterCavaPluginManagerMapPtr,
            resourceReaderPtr, clusterSorterManangersPtr, clusterSorterNamesPtr);
    qrsChainManagerConfig.setMetricsReporter(pluginMetricsReporter);
    QrsChainManagerPtr qrsChainMgrPtr = qrsChainManagerConfig.create(qrsConfig);
    return qrsChainMgrPtr;
}

AnalyzerFactoryPtr QrsChainManagerCreator::createAnalyzerFactory(
        const ResourceReaderPtr &resourceReader)
{
    return AnalyzerFactory::create(resourceReader);
}

bool QrsChainManagerCreator::createSorterManager(
        ResourceReaderPtr &resourceReader,
        const ConfigAdapterPtr &configAdapterPtr,
        ClusterSorterManagersPtr &clusterSorterManagersPtr,
        const ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
        MetricsReporter *metricsReporter)
{
    ClusterTableInfoMap::const_iterator it = clusterTableInfoMapPtr->begin();
    for (;it != clusterTableInfoMapPtr->end(); ++it) {
        const string &clusterName = it->first;
        if (StringUtil::endsWith(clusterName, HA3_DEFAULT_AGG)) {
            continue;
        }
        SorterManagerPtr sortManagerPtr =
            doCreateSorterManager(clusterName, resourceReader, configAdapterPtr, metricsReporter);
        if (NULL == sortManagerPtr) {
            HA3_LOG(ERROR, "create sort manager for cluster[%s] failed.",
                    (*it).first.c_str());
            return false;
        }
        (*clusterSorterManagersPtr)[clusterName] = sortManagerPtr;
    }

    return true;
}

SorterManagerPtr QrsChainManagerCreator::doCreateSorterManager(
        const string &clusterName, ResourceReaderPtr &resourceReader,
        const ConfigAdapterPtr &configAdapterPtr,
        MetricsReporter *metricsReporter)
{
    FinalSorterConfig finalSorterConfig;
    if (!configAdapterPtr->getFinalSorterConfig(clusterName, finalSorterConfig))
    {
        HA3_LOG(ERROR, "Get config section [final_sorter_config] in cluster[%s] failed.",
                clusterName.c_str());
        return SorterManagerPtr();
    }
    // add buildin sorters
    build_service::plugin::ModuleInfo moduleInfo;
    finalSorterConfig.modules.push_back(moduleInfo);
    suez::turing::SorterInfo sorterInfo;
    sorterInfo.sorterName = "DEFAULT";
    sorterInfo.className = "DefaultSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    sorterInfo.sorterName = "NULL";
    sorterInfo.className = "NullSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    return SorterManager::create(resourceReader->getConfigPath(), finalSorterConfig);
}

void QrsChainManagerCreator::getClusterSorterNames(
        const ClusterSorterManagersPtr &clusterSorterManangersPtr,
        ClusterSorterNamesPtr &clusterSorterNamesPtr)
{
    clusterSorterNamesPtr->clear();
    for (ClusterSorterManagers::const_iterator it =
             clusterSorterManangersPtr->begin();
         it != clusterSorterManangersPtr->end(); ++it)
    {
        it->second->getSorterNames((*clusterSorterNamesPtr)[it->first]);
    }
}

ClusterFuncMapPtr QrsChainManagerCreator::createClusterFuncMap(
        const std::map<std::string, suez::turing::FuncConfig> &funcConfigMap,
        config::ResourceReaderPtr &resourceReader,
        suez::turing::CavaJitWrapper *cavaJitWrapper)
{

    ClusterFuncMapPtr clusterFuncMapPtr(new ClusterFuncMap());
    for (map<string, FuncConfig>::const_iterator it = funcConfigMap.begin();
         it != funcConfigMap.end(); ++it)
    {
        auto &funcInfoMap = (*clusterFuncMapPtr)[it->first];
        FunctionInterfaceCreator creator;
        FunctionCreatorResource creatorResource;
        creatorResource.resourceReader = resourceReader;
        creatorResource.cavaJitWrapper = cavaJitWrapper;
        if (!creator.init(it->second, creatorResource)) {
            HA3_LOG(ERROR, "get function info map failed, clusterName[%s]", it->first.c_str());
            return ClusterFuncMapPtr();
        }
        funcInfoMap = creator.getFuncInfoMap();
    }
    return clusterFuncMapPtr;
}

CavaPluginManagerMapPtr QrsChainManagerCreator::createCavaPluginManagerMap(
        const std::map<std::string, suez::turing::CavaConfig> &cavaConfigMap,
        const std::map<std::string, suez::turing::FuncConfig> &funcConfigMap,
        config::ResourceReaderPtr &resourceReader,
        MetricsReporter *metricsReporter)
{
    CavaPluginManagerMapPtr cavaPluginManagerMapPtr(new CavaPluginManagerMap());
    for (auto it : cavaConfigMap) {
        auto cavaConfig = it.second;
        if (!cavaConfig._enable) {
            continue;
        }
        CavaJitWrapperPtr cavaJitWrapper(
                new CavaJitWrapper(resourceReader->getConfigPath(), cavaConfig, metricsReporter));
        if (!cavaJitWrapper->init()) {
            HA3_LOG(ERROR, "init cava jit wrapper failed, clusterName[%s]", it.first.c_str());
            return CavaPluginManagerMapPtr();
        }

        auto iter = funcConfigMap.find(it.first);
        CavaPluginManagerPtr cavaPluginManagerPtr(new CavaPluginManager(cavaJitWrapper, metricsReporter));
        if (!cavaPluginManagerPtr->init(iter->second._cavaFunctionInfos)) {
            HA3_LOG(ERROR, "init cava plugin manage failed, clusterName[%s]", it.first.c_str());
            return CavaPluginManagerMapPtr();
        }
        (*cavaPluginManagerMapPtr)[it.first] = cavaPluginManagerPtr;
    }
    return cavaPluginManagerMapPtr;
}

END_HA3_NAMESPACE(service);
