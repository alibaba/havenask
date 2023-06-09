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
#include "build_service/util/TimeTracer.h"

#include "autil/TimeUtility.h"

namespace build_service::util {

BS_LOG_SETUP(build_service.admin, TimeTracer);
TimeTracer::TimeTracer(const std::string& name) : _name(name)
{
    _tracers.emplace_back(std::make_pair("begin", autil::TimeUtility::currentTime()));
}
TimeTracer::~TimeTracer()
{
    _tracers.emplace_back(std::make_pair("end", autil::TimeUtility::currentTime()));

    std::stringstream ss;
    ss << "[";

    if (_tracers.size() > 2) {
        auto begin = _tracers.begin()->second;
        for (size_t i = 1; i < _tracers.size() - 1; ++i) {
            const auto& [k, v] = _tracers[i];
            ss << k << " : " << v - begin;
            begin = v;
            if (likely(i != _tracers.size() - 1)) {
                ss << ", ";
            }
        }
    }
    ss << "total: " << _tracers.rbegin()->second - _tracers.begin()->second << "]";
    BS_LOG(ERROR, "%s timeused %s", _name.c_str(), ss.str().c_str());
}

void TimeTracer::appendTrace(const std::string& key)
{
    _tracers.emplace_back(std::make_pair(key, autil::TimeUtility::currentTime()));
}

} // namespace build_service::util
