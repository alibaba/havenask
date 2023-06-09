/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/qrs/QrsChainManagerConfigurator.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/ProcessorInfo.h"
#include "ha3/config/QrsChainInfo.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/isearch.h"
#include "ha3/qrs/QrsChainConfigurator.h"
#include "ha3/qrs/QrsChainManager.h"
#include "ha3/qrs/QrsModuleFactory.h"
#include "ha3/service/HitSummarySchemaCache.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/common/RuntimeState.h"

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace std;
using namespace autil::legacy;
using namespace kmonitor;
using namespace isearch::util;
using namespace build_service::plugin;
using namespace isearch::service;
using namespace isearch::config;
using namespace build_service::analyzer;
using namespace multi_call;
using namespace suez::turing;
using namespace fslib::util;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, QrsChainManagerConfigurator);

void QrsChainManagerConfigurator::setMetricsReporter(MetricsReporter *metricsReporter) {
    _metricsReporter = metricsReporter;
}

QrsChainManagerPtr QrsChainManagerConfigurator::createFromFile(const string &filePath) {
    string configStr = FileUtil::readFile(filePath);
    if (configStr == ""){
        AUTIL_LOG(ERROR, "Can't open file [%s] or the file is empty.", filePath.c_str());
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
        AUTIL_LOG(ERROR, "qrsConfig FromJsonString fail:[%s]. exception:[%s]",
                content.c_str(), e.what());
        return QrsChainManagerPtr();
    }
    return create(qrsConfig);
}

QrsChainManagerPtr QrsChainManagerConfigurator::create(QrsConfig& qrsConfig)
{
    addDebugQrsChain(qrsConfig);
    QrsChainManagerPtr qrsChainManagerPtr(new QrsChainManager(_resourceReaderPtr));
    if (!qrsChainManagerPtr->addModules(qrsConfig._modules)) {
        AUTIL_LOG(ERROR, "Load qrs module info failed : %s",
                ToJsonString(qrsConfig._modules).c_str());
        return QrsChainManagerPtr();
    }

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
        AUTIL_LOG(DEBUG, "Add qrschaininfo: [%s]", it->getChainName().c_str());
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
        AUTIL_LOG(DEBUG, "QrsChainManagerPtr init success!");
        return qrsChainManagerPtr;
    } else {
        AUTIL_LOG(ERROR, "QrsChainManagerPtr init fail!");
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
} // namespace qrs
} // namespace isearch
