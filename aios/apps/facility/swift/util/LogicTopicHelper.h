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

#include <stdint.h>
#include <string>

#include "autil/Log.h"

namespace swift {
namespace util {

class LogicTopicHelper {
public:
    static std::string genPhysicTopicName(const std::string &logicName, int64_t timestamp, uint32_t partCnt);
    static bool getLogicTopicName(const std::string &physicName, std::string &logicName, bool needLog = true);
    static bool parsePhysicTopicName(const std::string &physicName,
                                     std::string &logicName,
                                     int64_t &timestamp,
                                     uint32_t &partCnt,
                                     bool needLog = true);
    static bool isValidPhysicTopicName(const std::string &physicName);
    static bool getPhysicPartCnt(const std::string &physicName, uint32_t &partCnt, bool needLog = true);
    static int64_t
    getPhysicTopicExpiredTime(int64_t physicNameTsUs, int64_t obsoleteFileTimeIntervalHour, int64_t brokerCfgTTlSec);

private:
    LogicTopicHelper();
    ~LogicTopicHelper();
    LogicTopicHelper(const LogicTopicHelper &);
    LogicTopicHelper &operator=(const LogicTopicHelper &);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
