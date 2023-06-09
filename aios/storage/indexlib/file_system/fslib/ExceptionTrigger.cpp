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
#include "indexlib/file_system/fslib/ExceptionTrigger.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "indexlib/util/Singleton.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, ExceptionTrigger);

const size_t ExceptionTrigger::NO_EXCEPTION_COUNT = 1000000000;
ExceptionTrigger::ExceptionTrigger() : _iOCount(0), _normalIOCount(NO_EXCEPTION_COUNT), _isPause(false) {}

ExceptionTrigger::~ExceptionTrigger() {}

bool ExceptionTrigger::TriggerException()
{
    ScopedLock lock(_lock);
    if (_isPause) {
        return false;
    }
    if (_iOCount > _normalIOCount) {
        return true;
    }
    _iOCount++;
    return false;
}

void ExceptionTrigger::Init(size_t normalIOCount)
{
    ScopedLock lock(_lock);
    _normalIOCount = normalIOCount;
    _iOCount = 0;
    _isPause = false;
}

size_t ExceptionTrigger::GetIOCount()
{
    ScopedLock lock(_lock);
    return _iOCount;
}

void ExceptionTrigger::Pause()
{
    ScopedLock lock(_lock);
    _isPause = true;
}

void ExceptionTrigger::Resume()
{
    ScopedLock lock(_lock);
    _isPause = false;
}

void ExceptionTrigger::InitTrigger(size_t normalIOCount)
{
    ExceptionTrigger* trigger = indexlib::util::Singleton<ExceptionTrigger>::GetInstance();
    trigger->Init(normalIOCount);
}

bool ExceptionTrigger::CanTriggerException()
{
    ExceptionTrigger* trigger = indexlib::util::Singleton<ExceptionTrigger>::GetInstance();
    return trigger->TriggerException();
}

size_t ExceptionTrigger::GetTriggerIOCount()
{
    return indexlib::util::Singleton<ExceptionTrigger>::GetInstance()->GetIOCount();
}

void ExceptionTrigger::PauseTrigger() { indexlib::util::Singleton<ExceptionTrigger>::GetInstance()->Pause(); }

void ExceptionTrigger::ResumeTrigger() { indexlib::util::Singleton<ExceptionTrigger>::GetInstance()->Resume(); }
}} // namespace indexlib::file_system
