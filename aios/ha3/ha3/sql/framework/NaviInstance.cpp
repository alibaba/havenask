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
#include "ha3/sql/framework/NaviInstance.h"
#include "ha3/sql/common/Log.h"

using namespace std;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(framework, NaviInstance);

NaviInstance &NaviInstance::get() {
    static NaviInstance instance;
    return instance;
}

bool NaviInstance::initNavi(multi_call::GigRpcServer *gigRpcServer,
                            kmonitor::MetricsReporter *metricsReporter) {
    SQL_LOG(INFO, "init navi.");
    _naviPtr.reset(new navi::Navi());
    if (metricsReporter) {
        _naviPtr->initMetrics(*metricsReporter);
    }
    if (!_naviPtr->init("", gigRpcServer)) {
        SQL_LOG(ERROR, "init global navi failed, please inspect navi.log");
        return false;
    }
    return true;
}

void NaviInstance::stopSnapshot() {
    SQL_LOG(INFO, "stop snapshot");
    if (_naviPtr) {
        _naviPtr->stopSnapshot();
    }
}

void NaviInstance::stopNavi() {
    SQL_LOG(INFO, "stop navi.");
    if (_naviPtr) {
        _naviPtr->stop();
    }
    _naviPtr.reset();
}

} //end namespace sql
} //end namespace isearch
