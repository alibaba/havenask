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
#include "build_service/admin/taskcontroller/PeriodMergeTrigger.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, PeriodMergeTrigger);

PeriodMergeTrigger::PeriodMergeTrigger() : _period(-1), _triggerTime(-1), _usePrecisePeriod(false) {}

PeriodMergeTrigger::PeriodMergeTrigger(const string& mergeConfigName, int64_t period)
    : _mergeConfigName(mergeConfigName)
    , _period(period)
    , _triggerTime(TimeUtility::currentTimeInSeconds() + period)
    , _usePrecisePeriod(false)
{
}

PeriodMergeTrigger::PeriodMergeTrigger(const string& mergeConfigName, int64_t period, int64_t nextTriggerTime)
    : _mergeConfigName(mergeConfigName)
    , _period(period)
    , _triggerTime(nextTriggerTime)
    , _usePrecisePeriod(true)
{
}

PeriodMergeTrigger::~PeriodMergeTrigger() {}

void PeriodMergeTrigger::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_config_name", _mergeConfigName);
    json.Jsonize("period", _period);
    json.Jsonize("next_trigger_time", _triggerTime);
    json.Jsonize("use_precise_period", _usePrecisePeriod, _usePrecisePeriod);
    if (json.GetMode() == FROM_JSON) {
        int64_t now = TimeUtility::currentTimeInSeconds();
        while (_triggerTime < now) {
            _triggerTime += _period;
        }
    }
}

bool PeriodMergeTrigger::triggerMergeTask(string& mergeTask)
{
    int64_t nowTime = TimeUtility::currentTimeInSeconds();
    if (nowTime >= _triggerTime) {
        mergeTask = _mergeConfigName;
        if (_usePrecisePeriod) {
            while (_triggerTime <= nowTime) {
                _triggerTime += _period;
            }
        } else {
            _triggerTime = nowTime + _period;
        }
        BS_LOG(INFO, "trigger merge task [%s]", mergeTask.c_str());
        return true;
    }
    return false;
}

bool PeriodMergeTrigger::operator==(const MergeTrigger& other) const
{
    const PeriodMergeTrigger* pm = dynamic_cast<const PeriodMergeTrigger*>(&other);
    if (!pm) {
        return false;
    }
    return _mergeConfigName == pm->_mergeConfigName && _period == pm->_period;
}

}} // namespace build_service::admin
