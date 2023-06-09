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

#include <stddef.h>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace indexlib { namespace file_system {

// TODO: RM
class ExceptionTrigger
{
public:
    ExceptionTrigger();
    ~ExceptionTrigger();

public:
    static void InitTrigger(size_t normalIOCount);
    static bool CanTriggerException();
    static size_t GetTriggerIOCount();
    static void PauseTrigger();
    static void ResumeTrigger();

    bool TriggerException();
    void Init(size_t normalIOCount);
    size_t GetIOCount();
    void Pause();
    void Resume();

public:
    static const size_t NO_EXCEPTION_COUNT;

private:
    mutable autil::ThreadMutex _lock;
    size_t _iOCount;
    size_t _normalIOCount;
    bool _isPause;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ExceptionTrigger> ExceptionTriggerPtr;
}} // namespace indexlib::file_system
