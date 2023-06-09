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
#include "indexlib/util/GroupedThreadPool.h"

#include "autil/LambdaWorkItem.h"
#include "autil/Lock.h"
#include "autil/WorkItem.h"
#include "autil/legacy/exception.h"

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib.util, GroupedThreadPool);

const std::string GroupedThreadPool::EXCEED_LIMIT_GROUP_NAME = "__EXCEED_LIMIT_GROUP__";

GroupedThreadPool::GroupedThreadPool() : _batchId(0), _maxGroupCount(0), _maxBatchCount(0) {}

GroupedThreadPool::~GroupedThreadPool() {}

indexlibv2::Status GroupedThreadPool::Start(const std::shared_ptr<autil::ThreadPool>& threadPool, size_t maxGroupCount,
                                            size_t maxBatchCount)
{
    _threadPool = threadPool;
    _maxGroupCount = maxGroupCount;
    _maxBatchCount = maxBatchCount;
    if (unlikely(threadPool == nullptr)) {
        return indexlibv2::Status::InternalError("thread pool is null");
    }
    if (unlikely(threadPool->getQueueSize() < _maxGroupCount + _maxBatchCount * threadPool->getThreadNum() + 1)) {
        return indexlibv2::Status::InternalError("illegal group thread pool param: queueSize [%lu] < maxGroupCount "
                                                 "[%lu] + maxBatchCount [%lu] * threadNum [%lu] +1",
                                                 threadPool->getQueueSize(), _maxGroupCount, _maxBatchCount,
                                                 threadPool->getThreadNum());
    }
    return indexlibv2::Status::OK();
}

void GroupedThreadPool::Start(const std::string& name, size_t threadNum, size_t maxGroupCount, size_t maxBatchCount)
{
    _maxGroupCount = maxGroupCount;
    _maxBatchCount = maxBatchCount;
    _threadPool = std::make_shared<autil::ThreadPool>(threadNum, maxGroupCount + maxBatchCount * threadNum + 1);
}

void GroupedThreadPool::StartNewBatch()
{
    while (true) {
        {
            autil::ScopedLock lock(_lock);
            if (_batchUnfinishWorkItemCounts.size() < _maxBatchCount) {
                ++_batchId;
                return;
            } else {
                AUTIL_LOG(INFO, "too many batches [%lu] >= MaxBatchCount [%lu], will wait",
                          _batchUnfinishWorkItemCounts.size(), _maxBatchCount);
            }
        }
        _batchFinishNotifier.waitNotification(10000);
    }
}

void GroupedThreadPool::PushTask(const std::string& group, autil::ThreadPool::Task task)
{
    auto lambdaWorkItem = std::make_unique<autil::LambdaWorkItem>(std::move(task));
    return PushWorkItem(group, std::move(lambdaWorkItem));
}

void GroupedThreadPool::PushWorkItem(const std::string& group, std::unique_ptr<autil::WorkItem> workItem)
{
    autil::ScopedLock lock(_lock);
    if (_workItems.count(group) == 0) {
        if (unlikely(_workItems.size() >= _maxGroupCount)) {
            // assert(false);  // comment for ut
            AUTIL_LOG(ERROR, "Too many groups exceed limit [%lu], will set group name to [%s]",
                      _threadPool->getQueueSize(), EXCEED_LIMIT_GROUP_NAME.c_str());
            if (unlikely(_workItems.count(EXCEED_LIMIT_GROUP_NAME) == 0)) {
                _workItems[EXCEED_LIMIT_GROUP_NAME] =
                    std::queue<std::pair<int32_t, std::unique_ptr<autil::WorkItem>>>();
            }
            DoPushWorkItem(EXCEED_LIMIT_GROUP_NAME, std::move(workItem));
            return;
        } else {
            _workItems[group] = std::queue<std::pair<int32_t, std::unique_ptr<autil::WorkItem>>>();
        }
    }
    DoPushWorkItem(group, std::move(workItem));
}

void GroupedThreadPool::DoPushWorkItem(const std::string& group, std::unique_ptr<autil::WorkItem> workItem)
{
    _workItems[group].push(std::make_pair(_batchId, std::move(workItem)));
    autil::WorkItem* workItemRawPtr = _workItems[group].back().second.get();
    _batchUnfinishWorkItemCounts[_batchId] =
        (_batchUnfinishWorkItemCounts.count(_batchId) > 0) ? _batchUnfinishWorkItemCounts[_batchId] + 1 : 1;
    if (_workItems[group].size() > 5) {
        AUTIL_LOG(DEBUG, "too many work items [%lu] for group [%s]", _workItems[group].size(), group.c_str());
    }
    if (_workItems[group].size() == 1) {
        _threadPool->pushTask(
            [this, group, batchId = _batchId, workItemRawPtr]() { DoWorkItem(group, batchId, workItemRawPtr); });
    }
}

bool GroupedThreadPool::AddBatchFinishHook(autil::ThreadPool::ThreadHook&& hook)
{
    autil::ScopedLock lock(_lock);
    if (_batchUnfinishWorkItemCounts.count(_batchId) == 0) {
        _threadPool->pushTask(hook);
        return true;
    }
    if (_batchFinishHooks.count(_batchId) == 0) {
        _batchFinishHooks[_batchId] = std::vector<autil::ThreadPool::ThreadHook>();
    }
    if (unlikely(_batchFinishHooks[_batchId].size() >= _threadPool->getThreadNum())) {
        // assert(false);     // comment for ut
        return false;
    }
    _batchFinishHooks[_batchId].emplace_back(hook);
    return true;
}

void GroupedThreadPool::DoWorkItem(const std::string& group, int32_t batchId, autil::WorkItem* workItem)
{
    try {
        workItem->process();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "group [%s] batch [%d] process exception [%s].", group.c_str(), batchId, e.what());
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "group [%s] batch [%d] process exception [%s]", group.c_str(), batchId, e.what());
    }
    autil::ScopedLock lock(_lock);
    assert(workItem == _workItems[group].front().second.get());
    _workItems[group].pop();
    _batchUnfinishWorkItemCounts[batchId] = _batchUnfinishWorkItemCounts[batchId] - 1;
    if (_batchUnfinishWorkItemCounts[batchId] == 0) {
        _batchFinishNotifier.notify();
        _batchUnfinishWorkItemCounts.erase(batchId);
        if (_batchFinishHooks.count(batchId) > 0) {
            for (const autil::ThreadPool::ThreadHook& hook : _batchFinishHooks[batchId]) {
                _threadPool->pushTask(hook);
            }
            _batchFinishHooks.erase(batchId);
        }
    }
    if (!_workItems[group].empty()) {
        int32_t nextWorkItemBatchId = _workItems[group].front().first;
        autil::WorkItem* nextWorkItem = _workItems[group].front().second.get();
        _threadPool->pushTask([this, group, nextWorkItemBatchId, nextWorkItem]() {
            DoWorkItem(group, nextWorkItemBatchId, nextWorkItem);
        });
    }
}

void GroupedThreadPool::WaitCurrentBatchWorkItemsFinish()
{
    while (true) {
        {
            autil::ScopedLock lock(_lock);
            if (_batchUnfinishWorkItemCounts.count(_batchId) == 0 || _batchUnfinishWorkItemCounts[_batchId] == 0) {
                return;
            }
        }
        _batchFinishNotifier.waitNotification(10000);
    }
}

} // namespace indexlib::util
