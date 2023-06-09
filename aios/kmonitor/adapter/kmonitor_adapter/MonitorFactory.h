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

#include <atomic>
#include <map>
#include <mutex>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace kmonitor_adapter {

class MonitorFactory
{
private:
    MonitorFactory();
    ~MonitorFactory();

public:
    Monitor* createMonitor(std::string serviceName);
    void close();

private:
    void init();
    static const char* getenv(const char* envName, const char* defaultValue);

public:
    static MonitorFactory* getInstance();

private:
    std::map<std::string, Monitor*> _monitorMap;
    std::atomic_bool _valid {false};
    bool _owner = false;
    static autil::RecursiveThreadMutex _mutex;
};

} // namespace kmonitor_adapter
