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
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/legacy/jsonizable.h"
#include "suez/common/TableMeta.h"
#include "suez/table/OperationType.h"
#include "suez/table/Table.h"
#include "suez/table/Todo.h"

namespace suez {

class TodoList : public autil::legacy::Jsonizable {
public:
    // for debug
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    std::string debugString() const;

public:
    void addOperation(const TablePtr &table, OperationType op);
    void addOperation(const TablePtr &table, OperationType op, const TargetPartitionMeta &target);
    void addOperation(const std::shared_ptr<Todo> &todo);
    bool needStopService() const;
    bool needCleanDisk() const;

    void maybeOptimize();

    std::set<PartitionId> getRemovedTables() const;

    size_t size() const;

    const std::map<OperationType, std::vector<std::shared_ptr<Todo>>> &getOperations() const { return _operations; }

    std::vector<Todo *> getOperations(const PartitionId &pid) const;

    bool remove(OperationType type, const PartitionId &pid);

private:
    bool hasOpType(OperationType type) const;

private:
    std::map<OperationType, std::vector<std::shared_ptr<Todo>>> _operations;
};

} // namespace suez
