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
#include <vector>

#include "autil/NoCopyable.h"
#include "build_service/util/Log.h"

namespace build_service::util {

class TimeTracer : private autil::NoCopyable
{
public:
    TimeTracer(const std::string& name);
    ~TimeTracer();

    void appendTrace(const std::string& key);

private:
    const std::string _name;
    std::vector<std::pair<std::string, int64_t>> _tracers;

private:
    BS_LOG_DECLARE();
};
} // namespace build_service::util
