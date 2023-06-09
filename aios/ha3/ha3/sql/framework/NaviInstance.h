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

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "navi/engine/Navi.h"

namespace isearch {
namespace sql {

class NaviInstance
{
public:
    NaviInstance(const NaviInstance &) = delete;
    NaviInstance& operator=(const NaviInstance &) = delete;
private:
    NaviInstance() = default;
    ~NaviInstance() = default;
public:
    static NaviInstance &get();
public:
    bool initNavi(multi_call::GigRpcServer *gigRpcServer,
                  kmonitor::MetricsReporter *metricsReporter);
    void stopSnapshot();
    void stopNavi();
    navi::Navi *getNavi() {
        return _naviPtr.get();
    }
private:
    navi::NaviPtr _naviPtr;
private:
    AUTIL_LOG_DECLARE();
};

} //end namespace sql
} //end namespace isearch
