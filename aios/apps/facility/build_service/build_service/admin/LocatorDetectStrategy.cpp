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
#include "build_service/admin/LocatorDetectStrategy.h"

#include <assert.h>
#include <cstdint>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, LocatorDetectStrategy);

const std::string LocatorDetectStrategy::CURRENT_MICROSECOND_CONDITION = "current_microsecond";

LocatorDetectStrategy::LocatorDetectStrategy(const std::string& condition, int64_t gap)
    : _condition(condition)
    , _gap(gap)
{
}

std::pair<bool, int64_t> LocatorDetectStrategy::getBaseValue() const
{
    if (_condition == CURRENT_MICROSECOND_CONDITION) {
        return {true, autil::TimeUtility::currentTimeInMicroSeconds()};
    }
    assert(false);
    return {false, 0};
}

bool LocatorDetectStrategy::detect(int64_t locator, const std::string& nodeIdentifier) const
{
    if (locator < 0) {
        BS_INTERVAL_LOG(300, DEBUG, "locator [%ld] is invalid", locator);
        return false;
    }
    auto [supportCondition, baseValue] = getBaseValue();
    if (!supportCondition) {
        BS_INTERVAL_LOG(300, INFO, "not support condition [%s]", _condition.c_str());
        return false;
    }

    if (baseValue - locator > _gap) {
        BS_LOG(WARN, "node [%s] is detect as slow node, base value [%ld], locator [%ld], gap [%ld]",
               nodeIdentifier.c_str(), baseValue, locator, _gap);
        return true;
    }
    BS_LOG(DEBUG, "node [%s] is not slow node, base value [%ld], locator [%ld], gap [%ld]", nodeIdentifier.c_str(),
           baseValue, locator, _gap);
    return false;
}

}} // namespace build_service::admin
