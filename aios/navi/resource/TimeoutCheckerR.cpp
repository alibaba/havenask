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
#include "navi/resource/TimeoutCheckerR.h"

namespace navi {

const std::string TimeoutCheckerR::RESOURCE_ID = "navi.timeout_checker_r";

TimeoutCheckerR::TimeoutCheckerR(TimeoutChecker *timeoutChecker)
    : _timeoutChecker(timeoutChecker)
{
}

TimeoutCheckerR::TimeoutCheckerR() {
}

TimeoutCheckerR::~TimeoutCheckerR() {
}

void TimeoutCheckerR::def(ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_GRAPH);
}

bool TimeoutCheckerR::config(ResourceConfigContext &ctx) {
    return true;
}

ErrorCode TimeoutCheckerR::init(ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

REGISTER_RESOURCE(TimeoutCheckerR);

}

