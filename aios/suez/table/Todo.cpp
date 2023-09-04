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
#include "suez/table/Todo.h"

#include "autil/EnvUtil.h"
#include "autil/HashUtil.h"
#include "autil/Log.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, Todo);

Todo::Todo(OperationType type, const std::string &identifier) : _type(type), _identifier(identifier) {
    _hashValue = autil::HashUtil::calculateHashValue(_identifier);
    autil::HashUtil::combineHash(_hashValue, _type);
}

Todo::~Todo() {}

void Todo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("op_type", opName(_type));
        json.Jsonize("identifier", _identifier);
    }
}

// TodoWithTable

TodoWithTable::TodoWithTable(OperationType type, const TablePtr &table)
    : Todo(type, table->getIdentifier()), _table(table) {}

void TodoWithTable::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        Todo::Jsonize(json);
        json.Jsonize("partition_id", _table->getPid());
        json.Jsonize("current", _table->getPartitionMeta());
    }
}

///////////////////////////// TodoWithTarget
TodoWithTarget::TodoWithTarget(OperationType type, const TablePtr &table, const TargetPartitionMeta &target)
    : TodoWithTable(type, table), _target(target) {}

void TodoWithTarget::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        TodoWithTable::Jsonize(json);
        json.Jsonize("target", _target);
    }
}

///////////////////////////////// operation implementation //////////////////
#define REGISTER_OP_WITH_TARGET(ClsName, opType, fn)                                                                   \
    class ClsName final : public TodoWithTarget {                                                                      \
    public:                                                                                                            \
        ClsName(const TablePtr &table, const TargetPartitionMeta &target) : TodoWithTarget(opType, table, target) {}   \
                                                                                                                       \
    public:                                                                                                            \
        void run() override { _table->fn(_target); }                                                                   \
    }

REGISTER_OP_WITH_TARGET(Init, OP_INIT, init);
REGISTER_OP_WITH_TARGET(Load, OP_LOAD, load);
REGISTER_OP_WITH_TARGET(Reload, OP_RELOAD, reload);
REGISTER_OP_WITH_TARGET(Preload, OP_PRELOAD, preload);
REGISTER_OP_WITH_TARGET(Forceload, OP_FORCELOAD, forceLoad);
REGISTER_OP_WITH_TARGET(UpdateRt, OP_UPDATERT, updateRt);

class UpdateKeepCount final : public TodoWithTarget {
public:
    UpdateKeepCount(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_UPDATEKEEPCOUNT, table, target) {}

public:
    void run() override { _table->setKeepCount(_target.getKeepCount()); }
};

class UpdateConfigKeepCount final : public TodoWithTarget {
public:
    UpdateConfigKeepCount(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_UPDATE_CONFIG_KEEPCOUNT, table, target) {}

public:
    void run() override { _table->setConfigKeepCount(_target.getConfigKeepCount()); }
};

class BecomeLeader final : public TodoWithTarget {
public:
    BecomeLeader(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_BECOME_LEADER, table, target) {}

public:
    void run() override {
        // TODO: force sync version from zookeeper to make leader always reopen the latest version
        // auto version = _versionMgr->getVersion(_table->getPid(), true);
        // _target.setVersion(version);
        _table->becomeLeader(_target);
    }
};

class NolongerLeader final : public TodoWithTarget {
public:
    NolongerLeader(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_NO_LONGER_LEADER, table, target) {}

public:
    void run() override { _table->nolongerLeader(_target); }
};

#undef REGISTER_OP_WITH_TARGET

class Deploy final : public TodoWithTarget {
public:
    Deploy(const TablePtr &table, const TargetPartitionMeta &target) : TodoWithTarget(OP_DEPLOY, table, target) {}

public:
    void run() override { _table->deploy(_target, false); }
};

class DistDeploy final : public TodoWithTarget {
public:
    DistDeploy(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_DIST_DEPLOY, table, target) {}

public:
    void run() override { _table->deploy(_target, true); }
};

class CleanDisk final : public TodoWithTarget {
public:
    CleanDisk(const TablePtr &table, const TargetPartitionMeta &target)
        : TodoWithTarget(OP_CLEAN_DISK, table, target) {}

public:
    void run() override { _table->deploy(_target, false); }
};

// CleanIncVersion
CleanIncVersion::CleanIncVersion(const TablePtr &table, const std::set<IncVersion> &inUseIncVersions)
    : TodoWithTable(OP_CLEAN_INC_VERSION, table), _inUseIncVersions(inUseIncVersions) {}

CleanIncVersion::~CleanIncVersion() {}

void CleanIncVersion::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    bool logEnabled = autil::EnvUtil::getEnv("debug_clean_version", false);
    if (logEnabled) {
        TodoWithTable::Jsonize(json);
    }
}

void CleanIncVersion::run() { _table->cleanIncVersion(_inUseIncVersions); }

class EmptyOpBase : public TodoWithTable {
public:
    EmptyOpBase(OperationType type, const TablePtr &table) : TodoWithTable(type, table) {}

public:
    void run() override {}
};

#define REGISTER_EMPTY_OP(ClsName, type)                                                                               \
    class ClsName final : public EmptyOpBase {                                                                         \
    public:                                                                                                            \
        ClsName(const TablePtr &table) : EmptyOpBase(type, table) {}                                                   \
    }

REGISTER_EMPTY_OP(None, OP_NONE);
REGISTER_EMPTY_OP(Hold, OP_HOLD);
REGISTER_EMPTY_OP(Remove, OP_REMOVE);

#undef REGISTER_EMPTY_OP

#define REGISTER_OP_WITHOUT_TARGET(ClsName, type, fn)                                                                  \
    class ClsName final : public TodoWithTable {                                                                       \
    public:                                                                                                            \
        ClsName(const TablePtr &table) : TodoWithTable(type, table) {}                                                 \
                                                                                                                       \
    public:                                                                                                            \
        void run() override { _table->fn(); }                                                                          \
    }

REGISTER_OP_WITHOUT_TARGET(CancelLoad, OP_CANCELLOAD, cancelLoad);
REGISTER_OP_WITHOUT_TARGET(Unload, OP_UNLOAD, unload);
REGISTER_OP_WITHOUT_TARGET(CancelDeploy, OP_CANCELDEPLOY, cancelDeploy);
REGISTER_OP_WITHOUT_TARGET(FinalTargetToTarget, OP_FINAL_TO_TARGET, finalTargetToTarget);

#undef REGISTER_OP_WITHOUT_TARGET

// creator
Todo *Todo::create(OperationType type, const TablePtr &table) {
    switch (type) {
    case OP_CANCELLOAD:
        return new CancelLoad(table);
    case OP_UNLOAD:
        return new Unload(table);
    case OP_CANCELDEPLOY:
        return new CancelDeploy(table);
    case OP_FINAL_TO_TARGET:
        return new FinalTargetToTarget(table);
    case OP_NONE:
        return new None(table);
    case OP_HOLD:
        return new Hold(table);
    case OP_REMOVE:
        return new Remove(table);
    default:
        AUTIL_LOG(ERROR, "unsupported operation type: %s", opName(type));
        return nullptr;
    }
}

Todo *Todo::createWithTarget(OperationType type, const TablePtr &table, const TargetPartitionMeta &target) {
    switch (type) {
    case OP_INIT:
        return new Init(table, target);
    case OP_LOAD:
        return new Load(table, target);
    case OP_RELOAD:
        return new Reload(table, target);
    case OP_PRELOAD:
        return new Preload(table, target);
    case OP_FORCELOAD:
        return new Forceload(table, target);
    case OP_FINAL_TO_TARGET:
        return new FinalTargetToTarget(table);
    case OP_UPDATEKEEPCOUNT:
        return new UpdateKeepCount(table, target);
    case OP_UPDATE_CONFIG_KEEPCOUNT:
        return new UpdateConfigKeepCount(table, target);
    case OP_UPDATERT:
        return new UpdateRt(table, target);
    case OP_BECOME_LEADER:
        return new BecomeLeader(table, target);
    case OP_NO_LONGER_LEADER:
        return new NolongerLeader(table, target);
    case OP_DEPLOY:
        return new Deploy(table, target);
    case OP_DIST_DEPLOY:
        return new DistDeploy(table, target);
    case OP_CLEAN_DISK:
        return new CleanDisk(table, target);
    default:
        // fallback to create without target
        AUTIL_LOG(DEBUG, "operation %s does not need target", opName(type));
        return create(type, table);
    }
}

} // namespace suez
