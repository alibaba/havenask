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

#include "navi/engine/Navi.h"
#include "suez_navi/resource/AsyncExecutorR.h"
#include <suez/sdk/SearchManager.h>
#include "kmonitor/client/MetricsReporterR.h"

namespace suez_navi {

class SuezTablePartInfos;

class NaviSearchManager : public suez::SearchManager {
public:
    NaviSearchManager();
    ~NaviSearchManager();
private:
    NaviSearchManager(const NaviSearchManager &);
    NaviSearchManager &operator=(const NaviSearchManager &);
public:
    bool init(const suez::SearchInitParam &initParam) override;
    suez::UPDATE_RESULT update(const suez::UpdateArgs &args,
                               suez::UpdateIndications &indications,
                               suez::SuezErrorCollector &errorCollector) override;
    void stopService() override;
    void stopWorker() override;
    autil::legacy::json::JsonMap getServiceInfo() const override;
private:
    bool initGrpcServer(multi_call::GigRpcServer *gigRpcServer);
    bool initInstallParam(const std::string &installRoot);
    void initMetrics(const suez::KMonitorMetaInfo &kmonMetaInfo);
    bool getLoadParam(const suez::IndexProvider &indexProvider,
                      const suez::BizMetas &bizMetas,
                      const suez::ServiceInfo &serviceInfo,
                      const autil::legacy::json::JsonMap &appInfo,
                      std::string &paramStr);
    bool getSuezTablePartInfos(const suez::IndexProvider &indexProvider, SuezTablePartInfos &partInfo) const;
private:
    std::string _installRoot;
    std::string _configLoader;
    navi::ResourcePtr _asyncExecutorR;
    std::unique_ptr<navi::Navi> _navi;
    kmonitor::MetricsReporterPtr _metricsReporter;
    kmonitor::MetricsReporterRPtr _metricsReporterR;
};

}
