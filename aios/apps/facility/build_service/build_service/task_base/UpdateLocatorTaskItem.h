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

#include "build_service/builder/Builder.h"
#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/util/TaskItem.h"

namespace build_service { namespace task_base {

class UpdateLocatorTaskItem : public indexlib::util::TaskItem
{
public:
    UpdateLocatorTaskItem(workflow::SwiftProcessedDocProducer* producer, builder::Builder* builder,
                          indexlib::util::MetricProviderPtr metricProvider = nullptr);
    ~UpdateLocatorTaskItem();

public:
    void Run() override;

private:
    UpdateLocatorTaskItem(const UpdateLocatorTaskItem&);
    UpdateLocatorTaskItem& operator=(const UpdateLocatorTaskItem&);

private:
    workflow::SwiftProcessedDocProducer* _producer;
    builder::Builder* _builder;
    common::Locator _locator;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(UpdateLocatorTaskItem);

}} // namespace build_service::task_base
