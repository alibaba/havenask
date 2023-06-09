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
#ifndef ISEARCH_BS_CONDITIONQUEUE_H
#define ISEARCH_BS_CONDITIONQUEUE_H

#include <functional>
#include <set>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace util {
enum POP_RESULT { PR_EOF = 0, PR_OK = 1, PR_NOVALID = 2 };

template <class T>
class ConditionQueue
{
public:
    static const uint32_t DEFAULT_QUEUE_SIZE = 1000;

public:
    ConditionQueue(uint32_t queueSize = DEFAULT_QUEUE_SIZE)
    {
        _finish = false;
        _forceStop = false;
        _offset = 0;
        _capacity = queueSize;
    }
    ~ConditionQueue() {}

private:
    ConditionQueue(const ConditionQueue&);
    ConditionQueue& operator=(const ConditionQueue&);
    static const uint32_t DEF_WAIT_TIME = 1000000; // 1 second

public:
    void push(const T& item)
    {
        autil::ScopedLock lk(_cond);
        if (_forceStop) {
            return;
        }
        while (_elements.size() >= _capacity) {
            _cond.wait(DEF_WAIT_TIME);
        }
        _elements.insert(std::make_pair(_offset++, item));
    }
    POP_RESULT pop(T& item, std::function<bool(const T& item)> func)
    {
        autil::ScopedLock lk(_cond);
        if (_forceStop) {
            return PR_EOF;
        }
        if (_finish && _elements.empty()) {
            return PR_EOF;
        }
        for (auto it = _elements.begin(); it != _elements.end(); ++it) {
            if (func(it->second)) {
                item = it->second;
                _elements.erase(it);
                _cond.signal();
                return PR_OK;
            }
        }
        return PR_NOVALID;
    }
    void setFinish() { _finish = true; }
    void setForceStop() { _forceStop = true; }
    size_t size()
    {
        autil::ScopedLock lk(_cond);
        return _elements.size();
    }
    void clear()
    {
        autil::ScopedLock lk(_cond);
        _elements.clear();
    }

private:
    std::set<std::pair<uint64_t, T>> _elements;
    autil::ThreadCond _cond;
    uint64_t _offset;
    uint32_t _capacity;
    std::atomic<bool> _finish;
    std::atomic<bool> _forceStop;

private:
    BS_LOG_DECLARE();
};
}} // namespace build_service::util

#endif // ISEARCH_BS_CONDITIONQUEUE_H
