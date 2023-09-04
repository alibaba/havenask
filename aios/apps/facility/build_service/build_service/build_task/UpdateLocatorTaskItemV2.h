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

#include "build_service/util/Log.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/util/TaskItem.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace build_service::workflow {
class SwiftProcessedDocProducer;
}

namespace build_service { namespace build_task {

class SingleBuilder;

class UpdateLocatorTaskItemV2 : public indexlib::util::TaskItem
{
public:
    UpdateLocatorTaskItemV2(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                            workflow::SwiftProcessedDocProducer* producer, SingleBuilder* builder);
    ~UpdateLocatorTaskItemV2();

    UpdateLocatorTaskItemV2(const UpdateLocatorTaskItemV2&) = delete;
    UpdateLocatorTaskItemV2& operator=(const UpdateLocatorTaskItemV2&) = delete;
    UpdateLocatorTaskItemV2(UpdateLocatorTaskItemV2&&) = delete;
    UpdateLocatorTaskItemV2& operator=(UpdateLocatorTaskItemV2&&) = delete;

    void Run() override;

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    workflow::SwiftProcessedDocProducer* _producer;
    SingleBuilder* _builder;
    indexlibv2::versionid_t _versionId = indexlibv2::INVALID_VERSIONID;
    indexlibv2::framework::Locator _locator;

    BS_LOG_DECLARE();
};

}} // namespace build_service::build_task
