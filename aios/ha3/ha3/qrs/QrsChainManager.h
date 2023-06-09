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
#pragma once

#include <map>
#include <memory>
#include <string>

#include "autil/CompressionUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/QrsChainInfo.h"
#include "ha3/config/QrsConfig.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/qrs/QrsChainConfig.h"
#include "ha3/qrs/QrsProcessor.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace config {
class ProcessorInfo;
}  // namespace config
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace isearch {
namespace qrs {

/** the class to manage processor chain in QRS service. */
class QrsChainManager : public build_service::plugin::PlugInManager
{
public:
    QrsChainManager(config::ResourceReaderPtr &resourceReaderPtr);
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
    void setRequestCompressType(autil::CompressType type) {
        _requestCompressType = type;
    }
    const autil::CompressType& getRequestCompressType() const {
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
    autil::CompressType _requestCompressType;
    config::ResourceReaderPtr _resourceReaderPtr;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
    suez::turing::ClusterSorterNamesPtr _clusterSorterNamesPtr;
    kmonitor::MetricsReporter *_metricsReporter;
private:
    friend class QrsChainManagerTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsChainManager> QrsChainManagerPtr;

} // namespace qrs
} // namespace isearch

