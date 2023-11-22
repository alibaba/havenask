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
#include "indexlib/indexlib.h"
#include "indexlib/partition/open_executor/open_executor.h"

namespace indexlib { namespace partition {

class ScopedLockExecutor : public OpenExecutor
{
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;

public:
    ScopedLockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& lock) : mScopedLock(scopedLock), mLock(lock) {}

    bool Execute(ExecutorResource& resource) override
    {
        mScopedLock.reset(new autil::ScopedLock(mLock));
        return true;
    }
    void Drop(ExecutorResource& resource) override { mScopedLock.reset(); }

private:
    ScopedLockPtr& mScopedLock;
    autil::ThreadMutex& mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ScopedLockExecutor);

class ScopedUnlockExecutor : public OpenExecutor
{
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;

public:
    ScopedUnlockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& lock) : mScopedLock(scopedLock), mLock(lock) {}

    bool Execute(ExecutorResource& resource) override
    {
        mScopedLock.reset();
        return true;
    }
    void Drop(ExecutorResource& resource) override { mScopedLock.reset(new autil::ScopedLock(mLock)); }

private:
    ScopedLockPtr& mScopedLock;
    autil::ThreadMutex& mLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ScopedUnlockExecutor);
}} // namespace indexlib::partition
