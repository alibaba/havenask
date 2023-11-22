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
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace admin {

class CounterCollector
{
public:
    CounterCollector();
    virtual ~CounterCollector();

private:
    CounterCollector(const CounterCollector&);
    CounterCollector& operator=(const CounterCollector&);

public:
    typedef std::map<std::string, proto::RoleCounterInfo> RoleCounterMap;

public:
    static CounterCollector* create(const config::CounterConfig& counterConfig, const proto::BuildId& buildId);

public:
    int64_t collect(RoleCounterMap& counterMap);
    bool syncCounters(bool forceSync = false);
    virtual bool clearCounters() = 0;
    void disableSyncCounter() { _disbleSyncCounter = true; }

private:
    virtual bool doSyncCounters(RoleCounterMap& counterMap) = 0;

protected:
    void fillRoleCounterMap(const std::string& roleName, const indexlib::util::CounterMapPtr& counterMap,
                            RoleCounterMap& roleCounterMap);

protected:
    proto::BuildId _buildId;

private:
    RoleCounterMap _roleCounterMap;
    uint64_t _lastSyncTime;
    mutable autil::ThreadMutex _lock;
    mutable bool _disbleSyncCounter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterCollector);

}} // namespace build_service::admin
