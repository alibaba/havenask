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

#include <memory>

#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncateWriterScheduler
{
public:
    TruncateWriterScheduler() {};
    virtual ~TruncateWriterScheduler() {};

public:
    virtual void PushWorkItem(autil::WorkItem* workItem) = 0;
    virtual void WaitFinished() = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateWriterScheduler);
} // namespace indexlib::index::legacy
