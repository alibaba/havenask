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
#include "fslib/fs/ExceptionTrigger.h"

#include <random>

#include "fslib/util/PathUtil.h"
#include "fslib/util/Singleton.h"

using namespace autil;
using std::string;

FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(fs);

AUTIL_DECLARE_AND_SETUP_LOGGER(fs, ExceptionTrigger);

const size_t ExceptionTrigger::NO_EXCEPTION_COUNT = 1000000000;
ExceptionTrigger::ExceptionTrigger()
    : mIOCount(0), mNormalIOCount(NO_EXCEPTION_COUNT), mIsPause(false)
    , mProbabilityMode(false), mTriggerOnceMode(false) {}

ExceptionTrigger::~ExceptionTrigger() {}

bool ExceptionTrigger::TriggerExceptionByCondition(const string &operation, const string &path, ErrorCode* ec) {
    // if conditions are not set, we should by default trigger.
    if (mPathToOperationEcMap.empty()) {
        return true;
    }
    if (operation.empty() && path.empty()) {
        return true;
    }
    string normalizedPath = PathUtil::normalizePath(path);
    bool found = false;
    string foundOperation;
    for (const auto &pair : mPathToOperationEcMap) {
        if (normalizedPath == pair.first || normalizedPath.find(pair.first) != string::npos) {
            found = true;
            *ec = pair.second.second;
            foundOperation = pair.second.first;
            break;
        }
    }
    if (!found) {
        return false;
    }
    // If operation is set to "", it means match any operation for the given path.
    return foundOperation == operation || foundOperation == "";
}

bool ExceptionTrigger::TriggerException(const string &operation, const string &path, ErrorCode* ec) {
    ScopedLock lock(mLock);
    if (mIsPause) {
        return false;
    }
    bool canTriggerByCondition = true;
    *ec = EC_NOTSUP;
    if(!operation.empty() || !path.empty()){
        canTriggerByCondition = TriggerExceptionByCondition(operation, path, ec);
    }
    if (mProbabilityMode) {
        return canTriggerByCondition && (size_t)(rand() % 100) >= mNormalIOCount;
    }

    if (!canTriggerByCondition) {
        return false;
    }
    if (mIOCount >= mNormalIOCount) {
        if (mTriggerOnceMode) {
            mNormalIOCount = NO_EXCEPTION_COUNT;
        }
        mIOCount++;
        return true;
    }
    mIOCount++;
    return false;
}

void ExceptionTrigger::Init(size_t normalIOCount, bool probabilityMode, bool triggerOnceMode) {
    ScopedLock lock(mLock);
    srand(time(NULL));

    mNormalIOCount = normalIOCount;
    mIOCount = 0;
    mIsPause = false;
    mProbabilityMode = probabilityMode;
    mTriggerOnceMode = triggerOnceMode;
}

size_t ExceptionTrigger::GetIOCount() {
    ScopedLock lock(mLock);
    return mIOCount;
}

void ExceptionTrigger::Pause() {
    ScopedLock lock(mLock);
    mIsPause = true;
}

void ExceptionTrigger::Resume() {
    ScopedLock lock(mLock);
    mIsPause = false;
}

void ExceptionTrigger::InitTrigger(size_t normalIOCount, bool probabilityMode, bool triggerOnceMode) {
    ExceptionTrigger *trigger = Singleton<ExceptionTrigger>::getInstance();
    trigger->Init(normalIOCount, probabilityMode, triggerOnceMode);
}

bool ExceptionTrigger::CanTriggerException(const string &operation, const string &fileName, ErrorCode* ec) {
    ExceptionTrigger *trigger = Singleton<ExceptionTrigger>::getInstance();
    return trigger->TriggerException(operation, fileName, ec);
}

size_t ExceptionTrigger::GetTriggerIOCount() { return Singleton<ExceptionTrigger>::getInstance()->GetIOCount(); }

void ExceptionTrigger::PauseTrigger() { Singleton<ExceptionTrigger>::getInstance()->Pause(); }

void ExceptionTrigger::ResumeTrigger() { Singleton<ExceptionTrigger>::getInstance()->Resume(); }

void ExceptionTrigger::SetExceptionCondition(const string &operation, const string &path, const ErrorCode& ec) {
    ExceptionTrigger *trigger = Singleton<ExceptionTrigger>::getInstance();
    return trigger->SetExceptionConditionInternal(operation, path, ec);
}

void ExceptionTrigger::SetExceptionConditionInternal(const string &operation, const string &path, const ErrorCode& ec) {
    if(path.empty() && operation.empty()){
        return;
    }
    mPathToOperationEcMap[PathUtil::normalizePath(path)] = std::make_pair(operation, ec);
}

FSLIB_END_NAMESPACE(fs);
