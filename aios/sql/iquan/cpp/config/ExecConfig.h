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

#include "iquan/common/Common.h"

namespace iquan {

class ParallelConfig {
public:
    ParallelConfig() : parallelNum(1) {}

    ParallelConfig(size_t parallelNum, const std::vector<std::string> &parallelTables)
        : parallelNum(parallelNum), parallelTables(parallelTables) {}

    ParallelConfig(const ParallelConfig &right) {
        parallelNum = right.parallelNum;
        parallelTables.clear();
        parallelTables.assign(right.parallelTables.begin(), right.parallelTables.end());
    }

    ParallelConfig &operator=(const ParallelConfig &right) {
        if (this == &right) {
            return *this;
        }
        parallelNum = right.parallelNum;
        parallelTables.clear();
        parallelTables.assign(right.parallelTables.begin(), right.parallelTables.end());
        return *this;
    }

    bool isValid() const {
        if (parallelNum <= 0) {
            return false;
        }
        return true;
    }

public:
    size_t parallelNum;
    std::vector<std::string> parallelTables;
};

class RuntimeConfig {
public:
    RuntimeConfig()
        : threadLimit(1)
        , timeout(1)
        , traceLevel("") // traceLevel
    {}

    RuntimeConfig(size_t threadLimit, size_t timeout, const std::string &traceLevel)
        : threadLimit(threadLimit), timeout(timeout), traceLevel(traceLevel) {}

    bool isValid() const {
        // todo: add trace level validatation
        if (threadLimit > 0 && timeout > 0) {
            return true;
        }
        return false;
    }

    RuntimeConfig(const RuntimeConfig &right) {
        threadLimit = right.threadLimit;
        timeout = right.timeout;
        traceLevel = right.traceLevel;
    }

    RuntimeConfig &operator=(const RuntimeConfig &right) {
        if (this == &right) {
            return *this;
        }
        threadLimit = right.threadLimit;
        timeout = right.timeout;
        traceLevel = right.traceLevel;
        return *this;
    }

public:
    size_t threadLimit;
    size_t timeout;
    std::string traceLevel;
};

class NaviRunGraphConfig {
public:
    NaviRunGraphConfig() : runtimeConfig(1, 1, "") {}

    NaviRunGraphConfig(const NaviRunGraphConfig &right) { runtimeConfig = right.runtimeConfig; }

    NaviRunGraphConfig &operator=(const NaviRunGraphConfig &right) {
        if (this == &right) {
            return *this;
        }
        runtimeConfig = right.runtimeConfig;
        return *this;
    }

    bool isValid() const { return runtimeConfig.isValid(); }

public:
    RuntimeConfig runtimeConfig;
};

class ExecConfig {
public:
    ExecConfig() : lackResultEnable(true) {}

    ExecConfig(const ExecConfig &right) {
        parallelConfig = right.parallelConfig;
        naviConfig = right.naviConfig;
        lackResultEnable = right.lackResultEnable;
    }

    ExecConfig &operator=(const ExecConfig &right) {
        if (this == &right) {
            return *this;
        }
        parallelConfig = right.parallelConfig;
        naviConfig = right.naviConfig;
        lackResultEnable = right.lackResultEnable;
        return *this;
    }

    bool isValid() const { return parallelConfig.isValid() && naviConfig.isValid(); }

public:
    ParallelConfig parallelConfig;
    NaviRunGraphConfig naviConfig;
    bool lackResultEnable;
};

} // namespace iquan
