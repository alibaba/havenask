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


#include <stdint.h>
#include <memory>
#include <string>

#include "autil/CompressionUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/service/QrsSearchConfig.h"
#include "ha3/turing/common/Ha3BizBase.h"
#include "ha3/turing/qrs/QrsRunGraphContext.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/lib/core/status.h"

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace config {
class QrsConfig;
}  // namespace config

namespace turing {

class QrsBiz: public Ha3BizBase
{
public:
    QrsBiz();
    ~QrsBiz();
private:
    QrsBiz(const QrsBiz &);
    QrsBiz& operator=(const QrsBiz &);

public:
    service::QrsSearchConfigPtr getQrsSearchConfig();
    config::ConfigAdapterPtr getConfigAdapter();
    kmonitor::MetricsReporter *getPluginMetricsReporter();
    QrsRunGraphContextArgs getQrsRunGraphContextArgs();
    suez::turing::QueryResourcePtr prepareQueryResource(autil::mem_pool::Pool *pool);
protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    suez::turing::QueryResourcePtr createQueryResource() override;
    bool updateFlowConfig(const std::string &zoneBizName) override;
    std::string getBizInfoFile() override;
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;

private:
    bool createQrsSearchConfig();
    autil::CompressType getCompressType(const config::QrsConfig &qrsConfig);
    bool updateHa3ClusterFlowConfig();
    bool updateTuringClusterFlowConfig();
public:
    static void fillCompatibleFieldInfo(config::AnomalyProcessConfig &flowConf);
private:
    service::QrsSearchConfigPtr _qrsSearchConfig;
    config::ConfigAdapterPtr _configAdapter;
    kmonitor::MetricsReporterPtr _pluginMetricsReporter;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsBiz> QrsBizPtr;

} // namespace turing
} // namespace isearch
