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

namespace indexlibv2::framework {

class TabletFault : public autil::legacy::Jsonizable
{
public:
    TabletFault() = default;
    TabletFault(const std::string& fault);
    ~TabletFault();

public:
    void Jsonize(JsonWrapper& json) override;
    void TriggerAgain();
    void Merge(const TabletFault& other);

    const std::string& GetFault() const { return _fault; }
    int64_t GetFirstTriggerTimeUs() const { return _firstTriggerTimeUs; }
    int64_t GetLastTriggerTimeUs() const { return _lastTriggerTimeUs; }
    int64_t GetTriggerCount() const { return _triggerCount; }
    bool operator==(const TabletFault& other) const;
    bool operator<(const TabletFault& other) const;

private:
    std::string _fault;
    int64_t _firstTriggerTimeUs = std::numeric_limits<int64_t>::max();
    int64_t _lastTriggerTimeUs = -1;
    int64_t _triggerCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

class TabletFaultManager : public autil::legacy::Jsonizable
{
public:
    TabletFaultManager() = default;
    ~TabletFaultManager() {};

public:
    void Jsonize(JsonWrapper& json) override;
    void TriggerFailure(const std::string& fault);
    std::optional<TabletFault> GetFault(const std::string& fault) const;
    size_t GetFaultCount() const;
    void MergeFault(const TabletFaultManager& other);

private:
    std::set<TabletFault> _faultSet;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
