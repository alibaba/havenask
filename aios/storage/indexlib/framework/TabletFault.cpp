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
#include "indexlib/framework/TabletFault.h"

#include "autil/TimeUtility.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletFault);
AUTIL_LOG_SETUP(indexlib.framework, TabletFaultManager);

TabletFault::TabletFault(const std::string& fault)
    : _fault(fault)
    , _firstTriggerTimeUs(autil::TimeUtility::currentTimeInMicroSeconds())
    , _lastTriggerTimeUs(_firstTriggerTimeUs)
    , _triggerCount(1)
{
}
void TabletFault::Jsonize(JsonWrapper& json)
{
    json.Jsonize("fault", _fault, _fault);
    json.Jsonize("first_trigger_time_in_us", _firstTriggerTimeUs, _firstTriggerTimeUs);
    json.Jsonize("last_trigger_time_in_us", _lastTriggerTimeUs, _lastTriggerTimeUs);
    json.Jsonize("trigger_count", _triggerCount, _triggerCount);
}

TabletFault::~TabletFault() {}

void TabletFault::TriggerAgain()
{
    _triggerCount++;
    _lastTriggerTimeUs = autil::TimeUtility::currentTimeInMicroSeconds();
}

void TabletFault::Merge(const TabletFault& other)
{
    if (_fault != other._fault) {
        return;
    }
    _triggerCount += other._triggerCount;
    _firstTriggerTimeUs = std::min(_firstTriggerTimeUs, other._firstTriggerTimeUs);
    _lastTriggerTimeUs = std::max(_lastTriggerTimeUs, other._lastTriggerTimeUs);
}

bool TabletFault::operator==(const TabletFault& other) const
{
    return _fault == other._fault && _firstTriggerTimeUs == other._firstTriggerTimeUs &&
           _lastTriggerTimeUs == other._lastTriggerTimeUs && _triggerCount == other._triggerCount;
}

bool TabletFault::operator<(const TabletFault& other) const { return _fault < other._fault; }

void TabletFaultManager::Jsonize(JsonWrapper& json) { json.Jsonize("fault_set", _faultSet, _faultSet); }
void TabletFaultManager::TriggerFailure(const std::string& fault)
{
    auto iter = _faultSet.find(TabletFault(fault));
    if (iter == _faultSet.end()) {
        _faultSet.insert(TabletFault(fault));
        return;
    }
    auto tabletFault = *iter;
    _faultSet.erase(iter);
    tabletFault.TriggerAgain();
    _faultSet.insert(tabletFault);
}
std::optional<TabletFault> TabletFaultManager::GetFault(const std::string& fault) const
{
    auto iter = _faultSet.find(TabletFault(fault));
    if (iter == _faultSet.end()) {
        return std::nullopt;
    }
    return *iter;
}

size_t TabletFaultManager::GetFaultCount() const { return _faultSet.size(); }
void TabletFaultManager::MergeFault(const TabletFaultManager& other)
{
    for (const auto& otherFault : other._faultSet) {
        auto iter = _faultSet.find(otherFault);
        if (iter == _faultSet.end()) {
            _faultSet.insert(otherFault);
            continue;
        }
        auto fault = *iter;
        _faultSet.erase(iter);
        fault.Merge(otherFault);
        _faultSet.insert(fault);
    }
}

} // namespace indexlibv2::framework
