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

#include <memory>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/admin/modules/SyncMetaModule.h"

namespace swift {
namespace monitor {
class AdminMetricsReporter;
} // namespace monitor
namespace admin {

class DispatchModule : public BaseModule {
public:
    DispatchModule();
    ~DispatchModule();
    DispatchModule(const DispatchModule &) = delete;
    DispatchModule &operator=(const DispatchModule &) = delete;

public:
    bool doInit() override;
    bool doLoad() override;
    bool doUnload() override;

private:
    void workLoop();

private:
    autil::LoopThreadPtr _loopThread;
    monitor::AdminMetricsReporter *_reporter;
    DEPEND_ON_MODULE(SyncMetaModule, _sysncMetaModule);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
