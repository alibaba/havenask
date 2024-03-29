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
#include "suez/table/TodoRunner.h"

#include "autil/Log.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TodoRunner);

TodoRunner::TodoRunner() : TodoRunner("") {}

TodoRunner::TodoRunner(const std::string &name) : _name(name) {}

TodoRunner::~TodoRunner() {}

SimpleRunner::SimpleRunner(const std::string &name) : TodoRunner(name) {}

bool SimpleRunner::run(const std::shared_ptr<Todo> &todo) {
    todo->run();
    return true;
}

AsyncRunner::AsyncRunner(autil::ThreadPool *threadPool) : AsyncRunner(threadPool, threadPool->getName()) {}

AsyncRunner::AsyncRunner(autil::ThreadPool *threadPool, const std::string &name)
    : TodoRunner(name), _threadPool(threadPool) {}

bool AsyncRunner::run(const std::shared_ptr<Todo> &todo) {
    if (!markOngoing(todo.get())) {
        AUTIL_LOG(INFO,
                  "todo [%s:%s] generated by last decision loop is still waiting to run",
                  todo->getIdentifier().c_str(),
                  opName(todo->getType()));
        return false;
    }
    auto fn = [this, todo]() {
        todo->run();
        remove(todo.get());
    };
    auto ret = _threadPool->pushTask(std::move(fn), false);
    if (ret != autil::ThreadPool::ERROR_NONE) {
        remove(todo.get());
        return false;
    }
    return true;
}

size_t AsyncRunner::ongoingOperationsCount() const {
    autil::ScopedLock lock(_mutex);
    return _ongoingOperations.size();
}

bool AsyncRunner::markOngoing(Todo *todo) {
    autil::ScopedLock lock(_mutex);
    auto ret = _ongoingOperations.emplace(todo->getIdentifier());
    return ret.second;
}

void AsyncRunner::remove(Todo *todo) {
    autil::ScopedLock lock(_mutex);
    _ongoingOperations.erase(todo->getIdentifier());
}

} // namespace suez
