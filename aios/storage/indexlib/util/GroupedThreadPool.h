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

#include <map>
#include <memory>
#include <queue>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "indexlib/base/Status.h"

namespace indexlib::util {

class GroupedThreadPool : private autil::NoCopyable
{
public:
    // @param maxGroupCount:
    // 最大的Group数量，超过之后，会自动将后续不重复的groupName退化成一个相同的默认值，影响并行效率
    // @param maxBatchCount: 同时运行的最大Batch数量，超过之后，后续的 StartNewBatch() 将同步等待
    GroupedThreadPool();
    ~GroupedThreadPool();

    GroupedThreadPool(const GroupedThreadPool&) = delete;
    GroupedThreadPool& operator=(const GroupedThreadPool&) = delete;
    GroupedThreadPool(GroupedThreadPool&&) = delete;
    GroupedThreadPool& operator=(GroupedThreadPool&&) = delete;

public:
    indexlibv2::Status Start(const std::shared_ptr<autil::ThreadPool>& threadPool, size_t maxGroupCount,
                             size_t maxBatchCount);
    void Legacy_Start(const std::string& name, size_t threadNum, size_t maxGroupCount, size_t maxBatchCount);

    void StartNewBatch();
    void PushTask(const std::string& group, autil::ThreadPool::Task task);
    void PushWorkItem(const std::string& group, std::unique_ptr<autil::WorkItem> workItem);

    // 属于该 batchId 的 workItem 都完成时的 hook
    // 1. 除非 Stop() 了线程池，hook 一定被执行且仅执行一次。
    // 2. hook 会被推入线程池等待执行，而不是在最后一个完成 workItem 的线程中执行。
    // 3. 如果调用 SetBatchFinishHook() 时 queue 中已不存在属于该 batch 的 workItem（没有添加过或已全部完成），那么 hook
    // 会被立即推入线程池等待执行
    // 4. 每个 batch 最多添加 threadNum 个 hook，当且仅当超过时返回 false
    bool AddBatchFinishHook(autil::ThreadPool::ThreadHook&& hook);

    // 同步等待所有的 workItem 和 hook 完成
    void WaitFinish();

    // 同步等待当前 Batch 的所有 workItem 完成，但不等待 BatchFinishHook 完成
    void WaitCurrentBatchWorkItemsFinish();

    // 强制停止并丢弃所有未完成的 workItem 和 hook
    void Stop();

    size_t GetThreadNum() { return _threadPool->getThreadNum(); }
    size_t GetGroupCount() { return _workItems.size(); }
    std::shared_ptr<autil::ThreadPool> Legacy_GetThreadPool() { return _threadPool; }

public:
    const std::map<std::string, std::queue<std::pair<int32_t, std::unique_ptr<autil::WorkItem>>>>* TEST_GetWorkItems()
    {
        return &_workItems;
    }

private:
    void DoPushWorkItem(const std::string& group, std::unique_ptr<autil::WorkItem> workItem);
    void DoWorkItem(const std::string& group, int32_t batchId, autil::WorkItem* workItem);
    indexlibv2::Status SetupLongTailFields();
    void PushToThreadPoolInternal(const std::string& group, int32_t batchId, autil::WorkItem* workItem);

private:
    int32_t _batchId;
    size_t _maxGroupCount;
    size_t _maxBatchCount;
    autil::ThreadMutex _lock;
    autil::Notifier _batchFinishNotifier;
    std::shared_ptr<autil::ThreadPool> _threadPool;
    // _longTailThreadPool is used to build fields that are configured by user which are known to be long tail.
    // This can be replaced if GroupThreadPool has smarter schedule algorithm, i.e. to do statistics on different items
    // and start slow items at higher priority to reduce its weight time, thus reducing overall run time of a batch.
    std::shared_ptr<autil::ThreadPool> _longTailThreadPool;
    // Group => { {BatchId1, workItem1}, {BatchId2, workItem2}, ...}
    std::map<std::string, std::queue<std::pair<int32_t, std::unique_ptr<autil::WorkItem>>>> _workItems;
    std::map<int32_t, int32_t> _batchUnfinishWorkItemCounts;
    std::map<int32_t, std::vector<autil::ThreadPool::ThreadHook>> _batchFinishHooks;
    std::vector<std::string> _longTailFieldsVec;

public:
    static const char* EXCEED_LIMIT_GROUP_NAME;
    static const char* LONG_TAIL_FIELD_ENV;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::util
