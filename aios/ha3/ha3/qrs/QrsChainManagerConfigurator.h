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

#include <stddef.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/analyzer/AnalyzerFactory.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/qrs/QrsChainManager.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace config {
class QrsConfig;
}  // namespace config
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace isearch {
namespace qrs {

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsChainManagerConfigurator> QrsChainManagerConfiguratorPtr;

} // namespace qrs
} // namespace isearch

