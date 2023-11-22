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
#include "build_service/workflow/RealTimeBuilderTaskItem.h"

#include <assert.h>
#include <iosfwd>

#include "build_service/workflow/RealtimeBuilderImplBase.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RealTimeBuilderTaskItem);

RealTimeBuilderTaskItem::RealTimeBuilderTaskItem(RealtimeBuilderImplBase* rtBuilderImpl) : _rtBuilderImpl(rtBuilderImpl)
{
}

RealTimeBuilderTaskItem::~RealTimeBuilderTaskItem() {}

void RealTimeBuilderTaskItem::Run()
{
    assert(_rtBuilderImpl);
    _rtBuilderImpl->executeBuildControlTask();
}

}} // namespace build_service::workflow
