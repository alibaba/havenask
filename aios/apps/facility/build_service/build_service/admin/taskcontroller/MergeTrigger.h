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

#include "autil/legacy/jsonizable.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MergeTrigger : public autil::legacy::Jsonizable
{
public:
    MergeTrigger();
    virtual ~MergeTrigger();

public:
    virtual bool triggerMergeTask(std::string& mergeTask) = 0;
    virtual bool operator==(const MergeTrigger& other) const = 0;

public:
    static MergeTrigger* create(const std::string& mergeConfigName, const std::string& description);

private:
    static MergeTrigger* createDaytimeMergeTrigger(const std::string& mergeConfigName,
                                                   const std::string& triggerTimeStr);
    static MergeTrigger* createPeriodMergeTrigger(const std::string& mergeConfigName, const std::string& periodStr);

private:
    static const std::string TIME_FORMAT;
    static const std::string PERIOD_KEY;
    static const std::string DAYTIME_KEY;

private:
    MergeTrigger(const MergeTrigger&);
    MergeTrigger& operator=(const MergeTrigger&);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
