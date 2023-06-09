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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib::file_system {

struct LifecyclePatternBase : public autil::legacy::Jsonizable {
    LifecyclePatternBase(bool isOffset) : isOffset(isOffset) {}
    virtual ~LifecyclePatternBase() {}

    static const std::string INTEGER_TYPE;
    static const std::string DOUBLE_TYPE;
    static const std::string STRING_TYPE;
    static const std::string CURRENT_TIME;

    std::string statisticType;
    std::string lifecycle;
    std::string statisticField;
    std::string offsetBase;
    bool isOffset;
    virtual bool InitOffsetBase(const std::map<std::string, std::string>& arguments) = 0;
};

struct IntegerLifecyclePattern : public LifecyclePatternBase {
    using RangeType = std::pair<int64_t, int64_t>;
    IntegerLifecyclePattern();
    ~IntegerLifecyclePattern();
    void Jsonize(JsonWrapper& json) override;
    bool InitOffsetBase(const std::map<std::string, std::string>& arguments) override;
    int64_t baseValue;
    RangeType range;
};

struct StringLifecyclePattern : public LifecyclePatternBase {
    StringLifecyclePattern();
    ~StringLifecyclePattern();
    bool InitOffsetBase(const std::map<std::string, std::string>& arguments) override { return true; }
    void Jsonize(JsonWrapper& json) override;
    std::vector<std::string> range;
};

class LifecycleConfig : public autil::legacy::Jsonizable
{
public:
    LifecycleConfig();
    LifecycleConfig(const LifecycleConfig&);
    LifecycleConfig& operator=(const LifecycleConfig&);
    ~LifecycleConfig();

public:
    using IntegerRangeType = std::pair<int64_t, int64_t>;
    using DoubleRangeType = std::pair<double, double>;

    static const std::string STATIC_STRATEGY;
    static const std::string DYNAMIC_STRATEGY;

public:
    void Jsonize(JsonWrapper& json) override;
    const std::string& GetStrategy() const;
    bool InitOffsetBase(const std::map<std::string, std::string>& arguments);
    const std::vector<std::shared_ptr<LifecyclePatternBase>>& GetPatterns() const;
    bool operator==(const LifecycleConfig& other) const;
    bool operator!=(const LifecycleConfig& other) const;
    bool EnableLocalDeployManifestChecking() const;

public:
    static bool IsRangeOverLapped(const IntegerRangeType& lhs, const IntegerRangeType& rhs);
    static int64_t CurrentTimeInSeconds();
    static void UpdateCurrentTimeBase(int64_t time);

private:
    struct LifecycleConfigImpl;
    std::unique_ptr<LifecycleConfigImpl> _impl;

private:
    static int64_t _TEST_currentTimeBase;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
