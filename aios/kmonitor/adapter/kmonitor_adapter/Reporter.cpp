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
#include "kmonitor_adapter/Reporter.h"

#include <cassert>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/Scope.h"
#include "autil/ThreadNameScope.h"

namespace kmonitor_adapter {

AUTIL_DECLARE_AND_SETUP_LOGGER(kmonitor, Reporter);

Reporter* Reporter::getInstance()
{
    static Reporter reporter;
    return &reporter;
}

Reporter::Reporter()
{
    // TODO: multi-threading report
    int64_t reportInterval = autil::EnvUtil::getEnv<size_t>("KMONITOR_ADAPTER_REPORT_INTERVAL",
                                                            1); // 1s
    if (reportInterval <= 0) {
        return;
    }
    _running = true;
    autil::ThreadNameScope _scope("KMonReport");
    _reportThread = std::thread([this, reportInterval]() {
        while (_running.load(std::memory_order_relaxed)) {
            if (_mtx.try_lock()) {
                autil::ScopeGuard scope([this]() { _mtx.unlock(); });
                for (auto recorder : _recorders) {
                    assert(recorder);
                    recorder->report();
                }
            }
            sleep(reportInterval); // by config
        }
    });
    AUTIL_LOG(INFO, "report thread started");
}
Reporter::~Reporter()
{
    if (_running.load()) {
        _running = false;
        _reportThread.join();
    }
}

void Reporter::registerRecorder(Recorder* r)
{
    std::lock_guard<std::mutex> l(_mtx);
    if (r) {
        _recorders.insert(r);
        AUTIL_LOG(INFO, "register recorder [%s]", r->name().c_str());
    }
}

void Reporter::unregisterRecoder(Recorder* r)
{
    std::lock_guard<std::mutex> l(_mtx);
    if (r) {
        _recorders.erase(r);
        AUTIL_LOG(INFO, "unregister recorder [%s]", r->name().c_str());
    }
}

Recorder::Recorder() : _threadData(new autil::ThreadLocalPtr(&unrefHandle)) {}
Recorder::~Recorder() {}

void Recorder::registerRecorder()
{
    auto reporter = Reporter::getInstance();
    assert(reporter);
    reporter->registerRecorder(this);
}

void Recorder::unregisterRecoder()
{
    auto reporter = Reporter::getInstance();
    assert(reporter);
    reporter->unregisterRecoder(this);
}

} // namespace kmonitor_adapter
