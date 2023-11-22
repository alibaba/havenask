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

#include <string>

#include "autil/Lock.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class CounterFileCollector : public CounterCollector
{
public:
    CounterFileCollector();
    ~CounterFileCollector();

private:
    CounterFileCollector(const CounterFileCollector&);
    CounterFileCollector& operator=(const CounterFileCollector&);

public:
    bool init(const std::string& zkRoot, const proto::BuildId& buildId);
    bool clearCounters() override;

private:
    bool doSyncCounters(RoleCounterMap& roleCounterMap) override;

private:
    std::string _zkCounterPath;
    mutable autil::ThreadMutex _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterFileCollector);

}} // namespace build_service::admin
