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

#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "indexlib/util/TaskItem.h"

namespace build_service { namespace workflow {

class RealtimeBuilderImplBase;

class RealTimeBuilderTaskItem : public indexlib::util::TaskItem
{
public:
    RealTimeBuilderTaskItem(RealtimeBuilderImplBase* rtBuilderImpl);
    ~RealTimeBuilderTaskItem();

public:
    void Run() override;

private:
    RealTimeBuilderTaskItem(const RealTimeBuilderTaskItem&);
    RealTimeBuilderTaskItem& operator=(const RealTimeBuilderTaskItem&);

private:
    RealtimeBuilderImplBase* _rtBuilderImpl;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealTimeBuilderTaskItem);

}} // namespace build_service::workflow
