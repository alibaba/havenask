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
#include "common/MultiThreadRunner.h"
#include "autil/WorkItem.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(common);

CARBON_LOG_SETUP(common, MultiThreadRunner);

class CounterWorkItem : public WorkItem {
public:
    CounterWorkItem(TerminateNotifier *counter, const std::function<void()> &func) {
        _counter = counter;
        _func = func;
    }
    ~CounterWorkItem(){}
    void process() override {
        _func();
    }
    void destroy() override {
        _counter->dec();
        delete this;
    }
    void drop() override {
        _counter->dec();
        delete this;
    }
private:
    std::function<void()> _func;
    TerminateNotifier *_counter;
};

MultiThreadRunner::MultiThreadRunner(const size_t threadNum, const size_t queueSize)
{
    _pool.reset(new ThreadPool(threadNum, queueSize));
}

MultiThreadRunner::~MultiThreadRunner() {
    if (_pool) {
        _pool->stop();
        _pool.reset();
    }
}

bool MultiThreadRunner::start() {
    if (!_pool) {
        CARBON_LOG(ERROR, "thread pool not inited.");
        return false;
    }
    return _pool->start();
}

void MultiThreadRunner::Run(const std::vector<std::function<void()>> &funcs) {
    TerminateNotifier counter;
    for (const auto &func : funcs) {
        counter.inc();
        _pool->pushWorkItem(new CounterWorkItem(&counter, func), true);
    }
    counter.wait();
}


END_CARBON_NAMESPACE(common);
