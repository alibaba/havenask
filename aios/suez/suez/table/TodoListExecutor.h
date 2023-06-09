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

#include "suez/table/TodoList.h"
#include "suez/table/TodoRunner.h"

namespace suez {

class TodoListExecutor {
public:
    TodoListExecutor();
    ~TodoListExecutor();

public:
    void execute(const TodoList &todoList, bool forceSync = false);
    void addRunner(std::unique_ptr<TodoRunner> runner, const std::set<OperationType> &attachedOps);
    bool assignToRunner(OperationType type, const std::string &runnerName);

private:
    // for test
    TodoRunner *getDefaultRunner() const { return _defaultRunner.get(); }
    TodoRunner *getRunner(OperationType type) const;

private:
    std::unique_ptr<TodoRunner> _defaultRunner;
    std::vector<std::unique_ptr<TodoRunner>> _runners;
    std::map<OperationType, TodoRunner *> _op2Runners;
};

} // namespace suez
