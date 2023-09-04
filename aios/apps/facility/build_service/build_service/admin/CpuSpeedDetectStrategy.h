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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class CpuSpeedDetectStrategy
{
public:
    const static constexpr int64_t CPU_FIXED_THREADHOLD = 1300000l;
    static int64_t INVALID_CPU_SPEED;

    struct Node {
        explicit Node(int64_t cpuSpeed) : cpuSpeed(cpuSpeed) {}
        Node(int64_t cpuSpeed, const std::string& ip, const std::string& roleName)
            : cpuSpeed(cpuSpeed)
            , ip(ip)
            , roleName(roleName)
        {
        }
        int64_t cpuSpeed = INVALID_CPU_SPEED;
        std::string ip;
        std::string roleName;
        std::map<std::string, std::string> comments;
    };

    explicit CpuSpeedDetectStrategy(const std::vector<int64_t>& allCpuSpeed, int64_t fixedValueThreshold = -1);
    ~CpuSpeedDetectStrategy() = default;

    CpuSpeedDetectStrategy(const CpuSpeedDetectStrategy&) = delete;
    CpuSpeedDetectStrategy& operator=(const CpuSpeedDetectStrategy&) = delete;
    CpuSpeedDetectStrategy(CpuSpeedDetectStrategy&&) = delete;
    CpuSpeedDetectStrategy& operator=(CpuSpeedDetectStrategy&&) = delete;

public:
    bool detect(Node& node);

private:
    bool triggerBoxplotDetect(Node& node);
    bool triggerMissDetect(Node& node);
    bool triggerFixedValueDetect(Node& node);
    bool triggerFalseDetect(Node& node);

private:
    static std::string BOXPLOT_DETECT;
    static std::string MISS_DETECT;
    static std::string FIXED_DETECT;
    static std::string FALSE_DETECT;
    static float MISS_DETECT_RATIO;
    static float FALSE_DETECT_RATIO;

    std::vector<int64_t> _allCpuSpeed;

    int64_t _fixedValueThreshold = INVALID_CPU_SPEED;
    int64_t _boxplotThreshold = INVALID_CPU_SPEED;
    int64_t _missDetectThreshold = INVALID_CPU_SPEED;
    int64_t _falseDetectThreshold = INVALID_CPU_SPEED;

    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
