#include <ha3/qrs/QrsChainManagerConfigurator.h>
#include <ha3/qrs/QrsChainConfigurator.h>
#include <suez/turing/common/FileUtil.h>
#include <string>
#include <ha3/config/QrsConfig.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/qrs/QrsModuleFactory.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

using namespace std;
using namespace autil::legacy;
using namespace kmonitor;
USE_HA3_NAMESPACE(util);
using namespace build_service::plugin;
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(config);
using namespace build_service::analyzer;
using namespace multi_call;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QrsChainManagerConfigurator);

void QrsChainManagerConfigurator::setMetricsReporter(MetricsReporter *metricsReporter) {
    _metricsReporter = metricsReporter;
}

QrsChainManagerPtr QrsChainManagerConfigurator::createFromFile(const string &filePath) {
    string configStr = FileUtil::readFile(filePath);
    if (configStr == ""){
        HA3_LOG(ERROR, "Can't open file [%s] or the file is empty.", filePath.c_str());
        return QrsChainManagerPtr();
    }
    return createFromString(configStr);
}

QrsChainManagerPtr QrsChainManagerConfigurator::createFromString(
        const std::string &content)
{
    QrsConfig qrsConfig;
    try{
        FromJsonString(qrsConfig, content);
    } catch (const ExceptionBase &e){
        HA3_LOG(ERROR, "qrsConfig FromJsonString fail:[%s]. exception:[%s]",
                content.c_str(), e.what());
        return QrsChainManagerPtr();
    }
    return create(qrsConfig);
}

QrsChainManagerPtr QrsChainManagerConfigurator::create(QrsConfig& qrsConfig)
{
    addDebugQrsChain(qrsConfig);
    _plugInManagerPtr.reset(new PlugInManager(_resourceReaderPtr, MODULE_FUNC_QRS));
    if (!_plugInManagerPtr->addModules(qrsConfig._modules)) {
        HA3_LOG(ERROR, "Load qrs module info failed : %s",
                ToJsonString(qrsConfig._modules).c_str());
        return QrsChainManagerPtr();
    }

    QrsChainManagerPtr qrsChainManagerPtr(new QrsChainManager(
                    _plugInManagerPtr, _resourceReaderPtr));
    qrsChainManagerPtr->setClusterConfigMap(_clusterConfigMapPtr);
    qrsChainManagerPtr->setQRSRule(qrsConfig._qrsRule);
    qrsChainManagerPtr->setRequestCompressType(
            qrsConfig._requestCompress.getCompressType());
    for (vector<ProcessorInfo>::const_iterator it = qrsConfig._processorInfos.begin();
         it != qrsConfig._processorInfos.end(); ++it){
        qrsChainManagerPtr->addProcessorInfo(*it);
    }

    for (vector<QrsChainInfo>::const_iterator it = qrsConfig._qrsChainInfos.begin();
         it != qrsConfig._qrsChainInfos.end(); ++it){
        HA3_LOG(DEBUG, "Add qrschaininfo: [%s]", it->getChainName().c_str());
        qrsChainManagerPtr->addQrsChainInfo(*it);
    }

    qrsChainManagerPtr->setClusterTableInfoMap(_clusterTableInfoMapPtr);
    qrsChainManagerPtr->setAnalyzerFactory(_analyzerFactoryPtr);
    qrsChainManagerPtr->setFuncInfoMap(_clusterFuncMapPtr);
    qrsChainManagerPtr->setCavaPluginManagerMap(_clusterCavaPluginManagerMapPtr);
    qrsChainManagerPtr->setClusterSorterManagers(_clusterSorterManagersPtr);
    qrsChainManagerPtr->setClusterSorterNames(_clusterSorterNamesPtr);
    qrsChainManagerPtr->setMetricsReporter(_metricsReporter);
    if (qrsChainManagerPtr->init()) {
        HA3_LOG(DEBUG, "QrsChainManagerPtr init success!");
        return qrsChainManagerPtr;
    } else {
        HA3_LOG(ERROR, "QrsChainManagerPtr init fail!");
        return QrsChainManagerPtr();
    }
}

void QrsChainManagerConfigurator::addDebugQrsChain(QrsConfig& qrsConfig) {
    string debugChainName = DEFAULT_DEBUG_QRS_CHAIN;
    string debugProcessorName = DEFAULT_DEBUG_PROCESSOR;
    bool isExist = false;
    vector<ProcessorInfo> &processorInfos = qrsConfig._processorInfos;
    for(size_t i = 0; i < processorInfos.size(); i++){
        if(processorInfos[i]._processorName == debugProcessorName){
            isExist = true;
            break;
        }
    }
    if(!isExist) {
        ProcessorInfo processorInfo;
        processorInfo._processorName = debugProcessorName;
        processorInfos.push_back(processorInfo);
    }
    vector<QrsChainInfo>& qrsChainInfos = qrsConfig._qrsChainInfos;
    for(int i = 0; i < (int)qrsChainInfos.size(); i++){
        if(qrsChainInfos[i]._chainName == debugChainName){
            qrsChainInfos.erase(qrsChainInfos.begin() + i);//remove build in qrs chain
            i--;
        }
    }

    QrsChainInfo debugChainInfo;
    debugChainInfo._chainName = debugChainName;
    debugChainInfo.addProcessor(BEFORE_SEARCH_POINT, debugProcessorName);
    qrsChainInfos.push_back(debugChainInfo);

}
END_HA3_NAMESPACE(qrs);
