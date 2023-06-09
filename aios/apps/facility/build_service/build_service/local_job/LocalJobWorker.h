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
#ifndef ISEARCH_BS_LOCALJOBWORKER_H
#define ISEARCH_BS_LOCALJOBWORKER_H

#include "build_service/common_define.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/util/Log.h"

namespace build_service { namespace local_job {

class LocalBrokerFactory;

class LocalJobWorker
{
public:
    LocalJobWorker();
    ~LocalJobWorker();

private:
    LocalJobWorker(const LocalJobWorker&);
    LocalJobWorker& operator=(const LocalJobWorker&);

public:
    bool run(const std::string& step, uint16_t mapCount, uint16_t reduceCount, const std::string& jobParams);

private:
    bool buildJob(uint16_t mapCount, uint16_t reduceCount, const std::string& jobParams, bool isTablet);
    bool mergeJob(uint16_t mapCount, uint16_t reduceCount, const std::string& jobParams, bool isTablet);
    bool endMergeJob(uint16_t mapCount, uint16_t reduceCount, const std::string& jobParams);
    bool parallelRunMergeTask(size_t instanceCount, const std::string& jobParams, task_base::TaskBase::Mode mode,
                              bool isTablet = false);
    bool downloadConfig(const std::string& configPath, std::string& localConfigPath);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::local_job

#endif // ISEARCH_BS_LOCALJOBWORKER_H
