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

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "suez/table/Todo.h"

namespace suez {

class TodoRunner {
public:
    TodoRunner();
    explicit TodoRunner(const std::string &name);
    virtual ~TodoRunner();

public:
    const std::string &getName() const { return _name; }
    virtual bool run(const std::shared_ptr<Todo> &todo) = 0;

private:
    std::string _name;
};

class SimpleRunner final : public TodoRunner {
public:
    explicit SimpleRunner(const std::string &name = "default");

public:
    bool run(const std::shared_ptr<Todo> &todo) override;
};

class AsyncRunner final : public TodoRunner {
public:
    explicit AsyncRunner(autil::ThreadPool *threadPool);
    AsyncRunner(autil::ThreadPool *threadPool, const std::string &name);

public:
    bool run(const std::shared_ptr<Todo> &todo) override;

    size_t ongoingOperationsCount() const;

private:
    bool markOngoing(Todo *todo);
    void remove(Todo *todo);

private:
    autil::ThreadPool *_threadPool;
    mutable autil::ThreadMutex _mutex;
    std::set<std::string> _ongoingOperations;
};

} // namespace suez
