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
#ifndef __FSLIB_EXCEPTION_TRIGGER_H
#define __FSLIB_EXCEPTION_TRIGGER_H

#include "autil/Log.h"
#include <memory>
#include "autil/Lock.h"

#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_BEGIN_NAMESPACE(fs);

class ExceptionTrigger
{
public:
    ExceptionTrigger();
    ~ExceptionTrigger();

public:
    // If probabilityMode=true, normalIOCount will be treated as probability p so that (100-p)/100 chance exception might
    // trigger.
    // when triggerOnceMode is true, io exception will only be triggered once.
    static void InitTrigger(size_t normalIOCount, bool probabilityMode = false, bool triggerOnceMode = false);
    static bool CanTriggerException(const std::string &operation, const std::string &path, ErrorCode* ec);
    // static bool CanTriggerException();
    static void PauseTrigger();
    static void ResumeTrigger();

public:
    static void SetExceptionCondition(const std::string& operation, const std::string& path, const ErrorCode& ec);

private:
   bool TriggerExceptionByCondition(const std::string &operation, const std::string &path, ErrorCode *ec);
   bool TriggerException(const std::string &operation, const std::string &path, ErrorCode *ec);
   void SetExceptionConditionInternal(const std::string &operation, const std::string &path, const ErrorCode &ec);
   static size_t GetTriggerIOCount();
   void Init(size_t normalIOCount, bool probabilityMode, bool triggerOnceMode);
   size_t GetIOCount();
   void Pause();
   void Resume();

public:
    static const size_t NO_EXCEPTION_COUNT;
private:
    mutable autil::ThreadMutex mLock;
    size_t mIOCount;
    size_t mNormalIOCount;
    bool mIsPause;
    bool mProbabilityMode;
    bool mTriggerOnceMode;

    std::map<std::string, std::pair<std::string, ErrorCode>> mPathToOperationEcMap;;

 private:
    friend class MergeExceptionUnittestTest;
};


FSLIB_TYPEDEF_AUTO_PTR(ExceptionTrigger);

FSLIB_END_NAMESPACE(fs);

#endif //__FSLIB_EXCEPTION_TRIGGER_H
