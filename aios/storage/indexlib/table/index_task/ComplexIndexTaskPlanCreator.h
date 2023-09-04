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
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"

namespace indexlibv2::framework {
class IndexTaskHistory;
class IIndexOperationCreator;
}; // namespace indexlibv2::framework

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::table {

class SimpleIndexTaskPlanCreator;

class ComplexIndexTaskPlanCreator : public framework::IIndexTaskPlanCreator
{
public:
    using CreateSimpleFunc =
        std::function<SimpleIndexTaskPlanCreator*(const std::string&, const std::map<std::string, std::string>&)>;

    struct SimpleTaskItem {
        explicit SimpleTaskItem(const std::string& taskType, const std::string& taskName = "",
                                const std::map<std::string, std::string>& params = {})
            : taskType(taskType)
            , taskName(taskName)
            , params(params)
        {
        }
        std::string taskType;
        std::string taskName;
        std::map<std::string, std::string> params;
        int64_t priority = 0;
    };

    ComplexIndexTaskPlanCreator() = default;
    ~ComplexIndexTaskPlanCreator() = default;

    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* taskContext) override;

protected:
    template <typename T>
    void RegisterSimpleCreator()
    {
        auto createFunc = [](const std::string& taskName,
                             const std::map<std::string, std::string>& params) -> SimpleIndexTaskPlanCreator* {
            SimpleIndexTaskPlanCreator* ret = new T(taskName, params);
            return ret;
        };
        _simpleCreatorFuncMap[T::TASK_TYPE] = std::move(createFunc);
    }

    virtual std::shared_ptr<framework::IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& tabletSchema) = 0;

    Status ScheduleSimpleTask(const framework::IndexTaskContext* taskContext, std::vector<SimpleTaskItem>* tasks);
    Status GetCandidateTasks(const framework::IndexTaskContext* taskContext, std::vector<SimpleTaskItem>* taskItems);

    virtual Status SelectTaskConfigs(const framework::IndexTaskContext* taskContext,
                                     std::vector<config::IndexTaskConfig>* configs);

protected:
    std::map<std::string, CreateSimpleFunc> _simpleCreatorFuncMap; /* key: type */

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
