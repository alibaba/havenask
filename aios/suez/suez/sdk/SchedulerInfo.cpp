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
#include "suez/sdk/SchedulerInfo.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SchedulerInfo);

namespace suez {

const string SchedulerInfo::UNINITIALIZED_VALUE = "uninitialized";

SchedulerInfo::SchedulerInfo() : _cm2TopoInfo(UNINITIALIZED_VALUE) {}

SchedulerInfo::~SchedulerInfo() {}

bool SchedulerInfo::operator==(const SchedulerInfo &info) const { return this->_cm2TopoInfo == info._cm2TopoInfo; }

bool SchedulerInfo::operator!=(const SchedulerInfo &info) const { return !(*this == info); }

void SchedulerInfo::Jsonize(JsonWrapper &json) { json.Jsonize("cm2_topo_info", _cm2TopoInfo, UNINITIALIZED_VALUE); }

bool SchedulerInfo::getCm2TopoInfo(string &cm2TopoInfo) const {
    if (_cm2TopoInfo == UNINITIALIZED_VALUE) {
        cm2TopoInfo.clear();
        return false;
    }
    cm2TopoInfo = _cm2TopoInfo;
    return true;
}

bool SchedulerInfo::parseFrom(const string &schedulerInfoStr) {
    if (schedulerInfoStr.empty()) {
        AUTIL_LOG(DEBUG, "empty schedulerInfoStr");
        return true;
    }
    try {
        FromJsonString(*this, schedulerInfoStr);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "e[%s], schedulerinfo str[%s] jsonize failed", e.what(), schedulerInfoStr.c_str());
        return false;
    }
    return true;
}

} // namespace suez
