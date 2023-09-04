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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/Log.h"

namespace autil {
class RoutingHashFunction : public HashFunctionBase {
public:
    RoutingHashFunction(const std::string &hashFunction,
                        const std::map<std::string, std::string> &params,
                        uint32_t partitionCount)
        : HashFunctionBase(hashFunction, partitionCount), _params(params), _routingRatio(0) {}
    virtual ~RoutingHashFunction() {}

public:
    bool init() override;
    uint32_t getHashId(const std::string &str) const override;
    uint32_t getHashId(const char *buf, size_t len) const override;
    uint32_t getHashId(const std::vector<std::string> &strVec) const override;
    std::vector<std::pair<uint32_t, uint32_t>> getHashRange(const std::vector<std::string> &strVec) const override;
    std::vector<std::pair<uint32_t, uint32_t>> getHashRange(uint32_t partId) const override;
    std::vector<std::pair<uint32_t, uint32_t>> getHashRange(uint32_t partId, float ratio) const override;

    float getRoutingRatio() { return _routingRatio; }

private:
    uint32_t getHashId(const std::string &str1, const std::string &str2) const;
    uint32_t getHashId(const std::string &str1, const std::string &str2, float ratio) const;
    std::vector<std::pair<uint32_t, uint32_t>> getInnerHashRange(const std::string &str) const;
    std::vector<std::pair<uint32_t, uint32_t>> getInnerHashRange(const std::string &str, float ratio) const;
    std::vector<std::pair<uint32_t, uint32_t>> getInnerHashRange(uint32_t hashVal, float ratio) const;
    bool parseHotValues(const std::string &key, const std::string &value);
    bool parseHotRanges(const std::string &key, const std::string &value);
    bool initFuncBase(const std::string &funcName);
    uint32_t getLogicId(const std::string &value) const;
    uint32_t getLogicId(const char *buf, size_t len) const;

private:
    std::map<std::string, std::string> _params;
    float _routingRatio;
    std::unordered_map<std::string, float> _hotValueMap;
    std::vector<float> _hotRangeVec;
    HashFunctionBasePtr _funcBase;

private:
    AUTIL_LOG_DECLARE();

private:
    friend class RoutingHashFunctionTest;
};

typedef std::shared_ptr<RoutingHashFunction> RoutingHashFunctionPtr;

} // namespace autil
