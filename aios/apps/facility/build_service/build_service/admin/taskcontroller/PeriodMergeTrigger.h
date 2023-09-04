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
#ifndef ISEARCH_BS_PERIODMERGETRIGGER_H
#define ISEARCH_BS_PERIODMERGETRIGGER_H

#include "build_service/admin/taskcontroller/MergeTrigger.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class PeriodMergeTrigger : public MergeTrigger
{
public:
    PeriodMergeTrigger(); // for jsonize
    PeriodMergeTrigger(const std::string& mergeConfigName, int64_t period);
    PeriodMergeTrigger(const std::string& mergeConfigName, int64_t period, int64_t nextTriggerTime);
    ~PeriodMergeTrigger();

private:
    PeriodMergeTrigger(const PeriodMergeTrigger&);
    PeriodMergeTrigger& operator=(const PeriodMergeTrigger&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool triggerMergeTask(std::string& mergeTask) override;
    bool operator==(const MergeTrigger& other) const override;

private:
    std::string _mergeConfigName;
    int64_t _period;
    int64_t _triggerTime;
    bool _usePrecisePeriod;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PeriodMergeTrigger);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PERIODMERGETRIGGER_H
