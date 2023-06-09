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
#ifndef ISEARCH_BS_MERGEINSTANCEWORKITEM_H
#define ISEARCH_BS_MERGEINSTANCEWORKITEM_H

#include "autil/WorkItem.h"
#include "build_service/task_base/MergeTask.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace build_service { namespace local_job {

class MergeInstanceWorkItem : public autil::WorkItem
{
public:
    MergeInstanceWorkItem(volatile bool* failflag, task_base::TaskBase::Mode mode, uint16_t instanceId,
                          const std::string& jobParams)
        : _failflag(failflag)
        , _mode(mode)
        , _instanceId(instanceId)
        , _jobParams(jobParams)
    {
    }
    ~MergeInstanceWorkItem() {}

private:
    MergeInstanceWorkItem(const MergeInstanceWorkItem&);
    MergeInstanceWorkItem& operator=(const MergeInstanceWorkItem&);

public:
    void process() override
    {
        if (!_mergeTask.run(_jobParams, _instanceId, _mode, true)) {
            std::string errorMsg = "run [" + task_base::TaskBase::getModeStr(_mode) + "] instance[" +
                                   autil::StringUtil::toString(_instanceId) + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            *_failflag = true;
        }
        _mergeTask.cleanUselessResource();
    }

private:
    volatile bool* _failflag;
    const task_base::TaskBase::Mode _mode;
    const uint16_t _instanceId;
    const std::string _jobParams;
    task_base::MergeTask _mergeTask;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::local_job

#endif // ISEARCH_BS_MERGEINSTANCEWORKITEM_H
