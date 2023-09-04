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
#include "sql/resource/TimeoutTerminatorR.h"

#include "autil/TimeoutTerminator.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/engine/TimeoutChecker.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/TimeoutCheckerR.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string TimeoutTerminatorR::RESOURCE_ID = "timeout_terminator_r";

TimeoutTerminatorR::TimeoutTerminatorR()
    : _timeoutTerminator(nullptr) {}

TimeoutTerminatorR::~TimeoutTerminatorR() {
    delete _timeoutTerminator;
}

void TimeoutTerminatorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_SUB_GRAPH);
}

bool TimeoutTerminatorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode TimeoutTerminatorR::init(navi::ResourceInitContext &ctx) {
    auto checker = _timeoutCheckerR->getTimeoutChecker();
    auto timeout = checker->timeoutMs() * 1000;
    auto startTime = checker->beginTime();
    _timeoutTerminator = new autil::TimeoutTerminator(timeout, startTime);
    _timeoutTerminator->init(1 << 12);
    NAVI_KERNEL_LOG(TRACE3,
                    "init timeout terminator, startTime[%ld] leftTime[%ld]",
                    startTime,
                    _timeoutTerminator->getLeftTime());
    return navi::EC_NONE;
}

REGISTER_RESOURCE(TimeoutTerminatorR);

} // namespace sql
