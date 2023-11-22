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

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/open_executor/open_executor.h"

namespace indexlib { namespace partition {

class LockExecutor : public OpenExecutor
{
public:
    LockExecutor(autil::ThreadMutex* lock) : mLock(lock) {}
    ~LockExecutor() {}

    bool Execute(ExecutorResource& resource) override
    {
        int ret = mLock->lock();
        assert(ret == 0);
        (void)ret;
        return true;
    }
    void Drop(ExecutorResource& resource) override
    {
        int ret = mLock->unlock();
        assert(ret == 0);
        (void)ret;
    }

private:
    autil::ThreadMutex* mLock;

private:
    IE_LOG_DECLARE();
};

class UnLockExecutor : public OpenExecutor
{
public:
    UnLockExecutor(autil::ThreadMutex* lock) : mLock(lock) {}
    ~UnLockExecutor() {}

    bool Execute(ExecutorResource& resource) override
    {
        int ret = mLock->unlock();
        assert(ret == 0);
        (void)ret;
        return true;
    }
    void Drop(ExecutorResource& resource) override
    {
        int ret = mLock->lock();
        assert(ret == 0);
        (void)ret;
    }

private:
    autil::ThreadMutex* mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LockExecutor);
}} // namespace indexlib::partition
