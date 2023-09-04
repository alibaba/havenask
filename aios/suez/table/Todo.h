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

#include "autil/legacy/jsonizable.h"
#include "suez/table/OperationType.h"
#include "suez/table/Table.h"

namespace suez {

class Todo : public autil::legacy::Jsonizable {
public:
    explicit Todo(OperationType type, const std::string &identifier);
    virtual ~Todo();

public:
    Todo(const Todo &other) = delete;
    Todo &operator=(const Todo &other) = delete;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    virtual void run() = 0;

    const std::string &getIdentifier() const { return _identifier; }
    OperationType getType() const { return _type; }
    uint64_t getHashValue() const { return _hashValue; }

public:
    template <typename Derived>
    Derived *cast() {
        return dynamic_cast<Derived *>(this);
    }

    template <typename Derived>
    const Derived *cast() const {
        return dynamic_cast<const Derived *>(this);
    }

public:
    static Todo *create(OperationType type, const TablePtr &table);
    static Todo *createWithTarget(OperationType type, const TablePtr &table, const TargetPartitionMeta &target);

private:
    OperationType _type;
    uint64_t _hashValue;
    std::string _identifier;
};

class TodoWithTable : public Todo {
public:
    TodoWithTable(OperationType type, const TablePtr &table);

public:
    const TablePtr &getTable() const { return _table; }
    const PartitionId &getPartitionId() const { return _table->getPid(); }
    // for debug
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

protected:
    TablePtr _table;
};

class TodoWithTarget : public TodoWithTable {
public:
    TodoWithTarget(OperationType type, const TablePtr &table, const TargetPartitionMeta &target);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    const TargetPartitionMeta &getTarget() const { return _target; }

protected:
    TargetPartitionMeta _target;
};

class CleanIncVersion final : public TodoWithTable {
public:
    CleanIncVersion(const TablePtr &table, const std::set<IncVersion> &inUseIncVersions);
    ~CleanIncVersion();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    void run() override;

private:
    std::set<IncVersion> _inUseIncVersions;
};

} // namespace suez
