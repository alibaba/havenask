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
#include "suez/table/TodoList.h"

#include <algorithm>

#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "suez/common/InnerDef.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TodoList);

void TodoList::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        for (const auto &it : _operations) {
            json.Jsonize(opName(it.first), it.second);
        }
    }
}

std::string TodoList::debugString() const { return autil::legacy::FastToJsonString(*this, true); }

static bool checkOp(OperationType op) {
    if (op == OP_INVALID) {
        return false;
    }
    return true;
}

void TodoList::addOperation(const TablePtr &table, OperationType op) {
    if (!checkOp(op)) {
        return;
    }
    std::shared_ptr<Todo> todo(Todo::create(op, table));
    addOperation(todo);
}

void TodoList::addOperation(const TablePtr &table, OperationType op, const TargetPartitionMeta &target) {
    if (!checkOp(op)) {
        return;
    }
    std::shared_ptr<Todo> todo(Todo::createWithTarget(op, table, target));
    addOperation(todo);
}

void TodoList::addOperation(const std::shared_ptr<Todo> &todo) {
    if (!todo) {
        AUTIL_LOG(ERROR, "todo is nullptr, not expected");
        return;
    }
    if (todo->getType() == OP_NONE) {
        return;
    }
    _operations[todo->getType()].emplace_back(todo);
}

bool TodoList::needCleanDisk() const { return hasOpType(OP_CLEAN_DISK); }

bool TodoList::needStopService() const {
    // TODO： 当前跟重构前版本兼容，是非常粗糙的
    //        除reload外，其他的异常处理基本是缺资源（比如memory，disk）导致
    //        正确的做法应该构建wait-for 有向图，如果图中有环，才走stop service逻辑
    //        否则，应该卡住
    if (hasOpType(OP_FORCELOAD) || hasOpType(OP_RELOAD)) {
        return true;
    }
    return false;
}

void TodoList::maybeOptimize() {
    // TODO: check current status is TS_UNLOADING in OP_HOLD
    //       <TS_UNLOADING, ET_REMOVE> -> OP_HOLD
    if (hasOpType(OP_FORCELOAD) && hasOpType(OP_UNLOAD)) {
        AUTIL_LOG(INFO, "delay force load as there are tables to be unloaded");
        _operations.erase(OP_FORCELOAD);
    }
}

std::set<PartitionId> TodoList::getRemovedTables() const {
    std::set<PartitionId> unused;
    for (const auto &it : _operations) {
        if (it.first != OP_REMOVE) {
            continue;
        }
        for (const auto &todo : it.second) {
            const TodoWithTable *typedTodo = todo->cast<TodoWithTable>();
            unused.insert(typedTodo->getPartitionId());
        }
    }
    return unused;
}

bool TodoList::hasOpType(OperationType type) const {
    auto it = _operations.find(type);
    return it != _operations.end() && !it->second.empty();
}

size_t TodoList::size() const {
    static const std::set<OperationType> IGNORE_OPS = {OP_NONE, OP_SYNC_VERSION};
    // ignore OP_NONE
    size_t count = 0;
    for (const auto &it : _operations) {
        if (IGNORE_OPS.count(it.first) == 0) {
            count += it.second.size();
        }
    }
    return count;
}

std::vector<Todo *> TodoList::getOperations(const PartitionId &pid) const {
    std::vector<Todo *> ret;
    for (const auto &it : _operations) {
        for (const auto &todo : it.second) {
            const TodoWithTable *typedTodo = todo->cast<TodoWithTable>();
            if (typedTodo != nullptr && pid == typedTodo->getPartitionId()) {
                ret.push_back(todo.get());
            }
        }
    }
    return ret;
}

bool TodoList::remove(OperationType type, const PartitionId &pid) {
    auto it = _operations.find(type);
    if (it == _operations.end()) {
        return false;
    }
    auto predicate = [&pid](const std::shared_ptr<Todo> &todo) {
        const TodoWithTable *typedTodo = todo->cast<TodoWithTable>();
        if (typedTodo != nullptr && pid == typedTodo->getPartitionId()) {
            return true;
        }
        return false;
    };
    auto &candidates = it->second;
    auto result = std::remove_if(candidates.begin(), candidates.end(), std::move(predicate));
    if (result != candidates.end()) {
        candidates.erase(result, candidates.end());
        return true;
    } else {
        return false;
    }
}

} // namespace suez
