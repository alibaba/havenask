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

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/analyzer/AnalyzerFactory.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/qrs/QrsChainManager.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor
namespace suez {
namespace turing {
class CavaConfig;
class CavaJitWrapper;
class FuncConfig;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {
class QrsBiz;
}  // namespace turing

namespace service {

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsChainManagerCreator> QrsChainManagerCreatorPtr;

} // namespace service
} // namespace isearch
