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
#ifndef ISEARCH_BS_BUILDINSTANCEWORKITEM_H
#define ISEARCH_BS_BUILDINSTANCEWORKITEM_H

#include "autil/StringUtil.h"
#include "autil/WorkItem.h"
#include "build_service/local_job/LocalBrokerFactory.h"
#include "build_service/task_base/BuildTask.h"

namespace build_service { namespace local_job {

class BuildInstanceWorkItem : public autil::WorkItem
{
public:
    BuildInstanceWorkItem(volatile bool* failflag, task_base::TaskBase::Mode mode, uint16_t instanceId,
                          const std::string& jobParams, ReduceDocumentQueue* queue, bool isTablet)
        : _failflag(failflag)
        , _mode(mode)
        , _instanceId(instanceId)
        , _jobParams(jobParams)
        , _localBrokerFactory(queue)
        , _buildTask("")
        , _isTablet(isTablet)
    {
        _localBrokerFactory.setTabletMode(isTablet);
    }
    ~BuildInstanceWorkItem() {}

private:
    BuildInstanceWorkItem(const BuildInstanceWorkItem&);
    BuildInstanceWorkItem& operator=(const BuildInstanceWorkItem&);

public:
    void process() override
    {
        if (!_buildTask.run(_jobParams, &_localBrokerFactory, _instanceId, _mode, _isTablet)) {
            std::string errorMsg = "run [" + task_base::TaskBase::getModeStr(_mode) + "] instance[" +
                                   autil::StringUtil::toString(_instanceId) + "] isTablet[" +
                                   autil::StringUtil::toString(_isTablet) + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            *_failflag = true;
        }
    }

private:
    volatile bool* _failflag;
    const task_base::TaskBase::Mode _mode;
    const uint16_t _instanceId;
    const std::string _jobParams;
    LocalBrokerFactory _localBrokerFactory;
    task_base::BuildTask _buildTask;
    bool _isTablet;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::local_job

#endif // ISEARCH_BS_BUILDINSTANCEWORKITEM_H
