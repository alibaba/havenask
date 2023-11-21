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
#include "indexlib/config/CustomIndexTaskClassInfo.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/framework/index_task/CustomIndexTaskFactoryCreator.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {

class CustomIndexTaskFactory : private autil::NoCopyable
{
public:
    CustomIndexTaskFactory() = default;
    virtual ~CustomIndexTaskFactory() = default;

    static std::pair<Status, std::unique_ptr<IIndexTaskPlanCreator>>
    GetCustomPlanCreator(const std::shared_ptr<config::IndexTaskConfig>& indexTaskConfig);
    static std::pair<Status, std::unique_ptr<framework::IIndexOperationCreator>>
    GetCustomOperationCreator(const std::shared_ptr<config::IndexTaskConfig>& indexTaskConfig,
                              const std::shared_ptr<config::ITabletSchema>& schema);

public:
    virtual std::unique_ptr<CustomIndexTaskFactory> Create() const = 0;
    virtual std::unique_ptr<IIndexOperationCreator>
    CreateIndexOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema) = 0;
    virtual std::unique_ptr<IIndexTaskPlanCreator> CreateIndexTaskPlanCreator() = 0;

private:
    inline AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.framework, CustomIndexTaskFactory);
};

inline std::pair<Status, std::unique_ptr<IIndexTaskPlanCreator>>
CustomIndexTaskFactory::GetCustomPlanCreator(const std::shared_ptr<config::IndexTaskConfig>& indexTaskConfig)
{
    auto [status, classInfo] = indexTaskConfig->GetSetting<config::CustomIndexTaskClassInfo>("class_info");
    if (!status.IsOK() && !status.IsNotFound()) {
        AUTIL_LOG(ERROR, "get class info failed");
        return {status, nullptr};
    }
    if (status.IsOK()) {
        // TODO(makuo.mnb): fix tablet schema
        auto customIndexTaskFactoryCreator = CustomIndexTaskFactoryCreator::GetInstance();
        auto className = classInfo.GetClassName();
        auto factory = customIndexTaskFactoryCreator->Create(className);
        if (!factory) {
            AUTIL_LOG(ERROR, "no task factory for [%s], registed typeds [%s]", className.c_str(),
                      autil::legacy::ToJsonString(customIndexTaskFactoryCreator->GetRegisteredType(), true).c_str());
            return {Status::Corruption(), nullptr};
        }
        auto customOpCreator = factory->CreateIndexTaskPlanCreator();
        if (customOpCreator == nullptr) {
            AUTIL_LOG(ERROR, "get custom op creator failed [%s]", classInfo.GetClassName().c_str());
            return {Status::Corruption(), nullptr};
        }
        return {Status::OK(), std::move(customOpCreator)};
    }
    return {Status::OK(), nullptr};
}

inline std::pair<Status, std::unique_ptr<framework::IIndexOperationCreator>>
CustomIndexTaskFactory::GetCustomOperationCreator(const std::shared_ptr<config::IndexTaskConfig>& indexTaskConfig,
                                                  const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto [status, classInfo] = indexTaskConfig->GetSetting<config::CustomIndexTaskClassInfo>("class_info");
    if (!status.IsOK() && !status.IsNotFound()) {
        AUTIL_LOG(ERROR, "get class info failed");
        return {status, nullptr};
    }
    if (status.IsOK()) {
        // TODO(makuo.mnb): fix tablet schema
        auto customIndexTaskFactoryCreator = CustomIndexTaskFactoryCreator::GetInstance();
        auto className = classInfo.GetClassName();
        auto factory = customIndexTaskFactoryCreator->Create(className);
        if (!factory) {
            AUTIL_LOG(ERROR, "no task factory for [%s], registed typeds [%s]", className.c_str(),
                      autil::legacy::ToJsonString(customIndexTaskFactoryCreator->GetRegisteredType(), true).c_str());
            return {Status::Corruption(), nullptr};
        }
        auto customOpCreator = factory->CreateIndexOperationCreator(schema);
        if (customOpCreator == nullptr) {
            AUTIL_LOG(ERROR, "get custom op creator failed [%s]", classInfo.GetClassName().c_str());
            return {Status::Corruption(), nullptr};
        }
        return {Status::OK(), std::move(customOpCreator)};
    }
    return {Status::OK(), nullptr};
}

/*
   添加一种新的FACTORY，需要完成以下2个步骤:
   1.在你的factory cpp内调用 REGISTER_CUSTOM_INDEX_TASK_FACTORY(factory_type, ClassName); 的宏，
       例如， REGISTER_CUSTOM_INDEX_TASK_FACTORY(Online2Offline, Online2OfflineTaskFactory);
   2.在该文件对应的目标BUILD目标上添加    alwayslink = True
*/

#define REGISTER_CUSTOM_INDEX_TASK_FACTORY(FACTORY_TYPE, TASK_FACTORY)                                                 \
    __attribute__((constructor)) void Register##FACTORY_TYPE##Factory()                                                \
    {                                                                                                                  \
        auto customIndexTaskFactoryCreator = indexlibv2::framework::CustomIndexTaskFactoryCreator::GetInstance();      \
        customIndexTaskFactoryCreator->Register(#FACTORY_TYPE, (new TASK_FACTORY));                                    \
    }

} // namespace indexlibv2::framework
