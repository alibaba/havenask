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
#include "suez/table/TodoListExecutor.h"

#include "autil/Log.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TodoListExecutor);

TodoListExecutor::TodoListExecutor() { _defaultRunner = std::make_unique<SimpleRunner>(); }

TodoListExecutor::~TodoListExecutor() {}

void TodoListExecutor::execute(const TodoList &todoList, bool forceSync) {
    for (const auto &it : todoList.getOperations()) {
        TodoRunner *runner = nullptr;
        if (forceSync) {
            runner = _defaultRunner.get();
        } else {
            auto type = it.first;
            runner = getRunner(type);
        }
        for (auto &todo : it.second) {
            runner->run(std::move(todo));
        }
    }
}

TodoRunner *TodoListExecutor::getRunner(OperationType type) const {
    auto it = _op2Runners.find(type);
    if (it == _op2Runners.end()) {
        return _defaultRunner.get();
    }
    return it->second;
}

void TodoListExecutor::addRunner(std::unique_ptr<TodoRunner> runner, const std::set<OperationType> &attachedOps) {
    for (auto op : attachedOps) {
        _op2Runners.emplace(op, runner.get());
    }
    _runners.emplace_back(std::move(runner));
}

bool TodoListExecutor::assignToRunner(OperationType type, const std::string &runnerName) {
    for (const auto &runner : _runners) {
        if (runnerName == runner->getName()) {
            _op2Runners.emplace(type, runner.get());
            return true;
        }
    }
    return false;
}

} // namespace suez
