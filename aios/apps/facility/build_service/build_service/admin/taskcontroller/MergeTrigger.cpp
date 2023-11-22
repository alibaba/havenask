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
#include "build_service/admin/taskcontroller/MergeTrigger.h"

#include <bits/types/struct_tm.h>
#include <ostream>
#include <stdint.h>
#include <time.h>

#include "alog/Logger.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "build_service/admin/taskcontroller/PeriodMergeTrigger.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, MergeTrigger);

const string MergeTrigger::TIME_FORMAT = "%H:%M";
const string MergeTrigger::PERIOD_KEY = "period";
const string MergeTrigger::DAYTIME_KEY = "daytime";

MergeTrigger::MergeTrigger() {}

MergeTrigger::~MergeTrigger() {}

MergeTrigger* MergeTrigger::create(const string& mergeConfigName, const string& description)
{
    StringTokenizer st(description, "=", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (2 != st.getNumTokens()) {
        stringstream ss;
        ss << "unrecognizable merge trigger description [" << description << "] for [" << mergeConfigName << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    if (DAYTIME_KEY == st[0]) {
        return createDaytimeMergeTrigger(mergeConfigName, st[1]);
    } else if (PERIOD_KEY == st[0]) {
        return createPeriodMergeTrigger(mergeConfigName, st[1]);
    } else {
        stringstream ss;
        ss << "unrecognizable merge trigger description [" << description << "] for [" << mergeConfigName << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
}

MergeTrigger* MergeTrigger::createDaytimeMergeTrigger(const string& mergeConfigName, const string& triggerTimeStr)
{
    struct tm triggerTime;
    char* p = strptime(triggerTimeStr.c_str(), TIME_FORMAT.c_str(), &triggerTime);
    if (p != &(*triggerTimeStr.end())) {
        string errorMsg = "fail to convert [" + triggerTimeStr + "] to time";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    time_t nowTime = time(NULL);
    struct tm nowTm;
    localtime_r(&nowTime, &nowTm);
    struct tm nextTriggerTm = nowTm;
    nextTriggerTm.tm_sec = 0;
    nextTriggerTm.tm_min = triggerTime.tm_min;
    nextTriggerTm.tm_hour = triggerTime.tm_hour;

    time_t nextTriggerTime = mktime(&nextTriggerTm);
    // move to tomorrow if today merge time already passed ...
    int64_t secondsInDay = 24 * 60 * 60;
    if (nextTriggerTime <= nowTime) {
        nextTriggerTime += secondsInDay;
    }

    return new PeriodMergeTrigger(mergeConfigName, secondsInDay, nextTriggerTime);
}

MergeTrigger* MergeTrigger::createPeriodMergeTrigger(const string& mergeConfigName, const string& periodStr)
{
    int64_t period;
    if (!StringUtil::fromString(periodStr, period)) {
        string errorMsg = "fail to convert [" + periodStr + "] to period time";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
    if (period <= 0) {
        string errorMsg = "period time [" + periodStr + "] illegal";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return NULL;
    }
    return new PeriodMergeTrigger(mergeConfigName, period);
}

}} // namespace build_service::admin
