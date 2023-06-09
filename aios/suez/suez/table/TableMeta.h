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

#include <algorithm>
#include <cstdio>
#include <iosfwd>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/indexlib.h"
#include "suez/sdk/PartitionId.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/SuezPartitionType.h"
#include "suez/sdk/TableUserDefinedMeta.h"
#include "suez/table/InnerDef.h"

namespace suez {

typedef schemavid_t SchemaVersion;

// tableIndexPath/TableA/generation_0/partition_0_1/version.0

enum TableMode {
    TM_NORMAL,
    TM_READ_WRITE,
};

struct TableMeta : public autil::legacy::Jsonizable {
public:
    TableMeta()
        : mode(TM_NORMAL)
        , tableType(SPT_NONE)
        , rtStatus(TRS_BUILDING)
        , forceOnline(false)
        , timestampToSkip(-1)
        , totalPartitionCount(-1) {}
    ~TableMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("table_mode", mode, mode);
        json.Jsonize("table_type", tableType, tableType);
        json.Jsonize("rt_status", rtStatus, rtStatus);
        // in c/s separate mode, offline dfs root, for deploy
        json.Jsonize("raw_index_root", rawIndexRoot, rawIndexRoot);
        json.Jsonize("force_online", forceOnline, forceOnline);
        json.Jsonize("timestamp_to_skip", timestampToSkip, timestampToSkip);
        json.Jsonize("total_partition_count", totalPartitionCount, totalPartitionCount);
        json.Jsonize("user_defined_param", userDefinedParam, userDefinedParam);

        // 兼容target
        json.Jsonize("index_root", indexRoot, indexRoot);
        json.Jsonize("config_path", configPath, configPath);
    }

    static bool needUpdateRt(const TableMeta &target, const TableMeta &current) {
        return target.rtStatus != current.rtStatus || target.timestampToSkip > current.timestampToSkip ||
               (target.forceOnline && !current.forceOnline);
    }

public:
    TableMode mode;
    SuezPartitionType tableType;
    TableRtStatus rtStatus;
    std::string rawIndexRoot;
    bool forceOnline;
    int64_t timestampToSkip;
    int32_t totalPartitionCount;
    autil::legacy::json::JsonMap userDefinedParam;
    std::string indexRoot;
    std::string configPath;
};

struct DeployInfo : public autil::legacy::Jsonizable {
    DeployInfo(DeployStatus ds = DS_UNKNOWN) : deployStatus(ds) {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("deploy_status", deployStatus, deployStatus);
    }

    DeployStatus deployStatus;
};

/*
EffectiveFieldInfo表示表可以提供服务的字段集合
*/
struct EffectiveFieldInfo : public autil::legacy::Jsonizable {
public:
    EffectiveFieldInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("attributes", attributes, attributes);
        json.Jsonize("indexes", indexes, indexes);
    }

public:
    void clear() {
        if (!attributes.empty()) {
            attributes.clear();
        }
        if (!indexes.empty()) {
            indexes.clear();
        }
    }
    void sort() {
        std::sort(attributes.begin(), attributes.end());
        std::sort(indexes.begin(), indexes.end());
    }
    bool contains(const EffectiveFieldInfo &target, const PartitionId &pid) const;

public:
    std::vector<std::string> attributes;
    std::vector<std::string> indexes;
};

struct DeployMeta {
public:
    DeployMeta() {}
    ~DeployMeta() {}

public:
    DeployMeta(const DeployMeta &other) { *this = other; }
    DeployMeta &operator=(const DeployMeta &other);

public:
    bool hasDeployingIncVersion() const {
        autil::ScopedLock lock(_mutex);
        for (const auto &kv : deployStatusMap) {
            if (kv.second.deployStatus == DS_DEPLOYING) {
                return true;
            }
        }
        return false;
    }
    void setDeployStatus(IncVersion incVersion, DeployStatus status) {
        autil::ScopedLock lock(_mutex);
        deployStatusMap[incVersion].deployStatus = status;
    }

    IncVersion getLatestDeployVersion() const;
    DeployStatus getLatestDeployStatus() const;
    DeployStatus getDeployStatus(IncVersion incVersion) const {
        autil::ScopedLock lock(_mutex);
        auto iter = deployStatusMap.find(incVersion);
        if (iter == deployStatusMap.end()) {
            return DS_UNKNOWN;
        }
        return iter->second.deployStatus;
    }

    std::set<IncVersion> getNeedKeepVersions(IncVersion incVersion) const {
        std::set<IncVersion> versions;
        autil::ScopedLock lock(_mutex);
        versions.insert(incVersion);
        for (const auto &it : deployStatusMap) {
            if (it.first >= incVersion) {
                versions.insert(it.first);
            }
        }
        return versions;
    }

    void clearIncVersion(const std::set<IncVersion> &inUseVersions);

    std::string getConfigPath() const {
        autil::ScopedLock lock(_mutex);
        return configPath;
    }
    void setConfigPath(const std::string &configPath_) {
        autil::ScopedLock lock(_mutex);
        configPath = configPath_;
    }
    void setIndexRoot(const std::string &indexRoot_) {
        autil::ScopedLock lock(_mutex);
        indexRoot = indexRoot_;
    }
    std::string getIndexRoot() const {
        autil::ScopedLock lock(_mutex);
        return indexRoot;
    }

    void setRawIndexRoot(const std::string &rawIndexRoot_) { rawIndexRoot = rawIndexRoot_; }

    std::string getRawIndexRoot() const {
        autil::ScopedLock lock(_mutex);
        if (!rawIndexRoot.empty()) {
            return rawIndexRoot;
        }
        return indexRoot;
    }

    std::map<IncVersion, DeployInfo> getDeployStatusMap() {
        autil::ScopedLock lock(_mutex);
        return deployStatusMap;
    }

private:
    mutable autil::ThreadMutex _mutex;

    std::map<IncVersion, DeployInfo> deployStatusMap;
    std::string configPath;
    std::string indexRoot;
    std::string rawIndexRoot;
};

struct PartitionMeta : public autil::legacy::Jsonizable {
public:
    PartitionMeta()
        : incVersion(INVALID_VERSION)
        , schemaVersion(0)
        , rollbackTimestamp(0)
        , branchId(0)
        , keepCount(1)
        , configKeepCount(1)
        , tableLoadType(TLT_TRY_LOAD_FIRST)
        , tableStatus(TS_UNKNOWN)
        , roleType(RT_FOLLOWER)
        , fullIndexLoaded(false) {
        deployMeta = std::make_shared<DeployMeta>();
    }
    ~PartitionMeta() {}

public:
    PartitionMeta(const PartitionMeta &other) { *this = other; }
    PartitionMeta &operator=(const PartitionMeta &other);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool validate() const;

    void setRtStatus(TableRtStatus status) {
        autil::ScopedLock lock(_mutex);
        tableMeta.rtStatus = status;
    }

    void setForceOnline(bool online) {
        autil::ScopedLock lock(_mutex);
        tableMeta.forceOnline = online;
    }

    bool getForceOnline() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.forceOnline;
    }

    void setTimestampToSkip(int64_t timestamp) {
        autil::ScopedLock lock(_mutex);
        tableMeta.timestampToSkip = timestamp;
    }

    int64_t getTimestampToSkip() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.timestampToSkip;
    }

    TableRtStatus getRtStatus() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.rtStatus;
    }
    void setTableStatus(TableStatus status) {
        autil::ScopedLock lock(_mutex);
        tableStatus = status;
    }
    TableStatus getTableStatus() const {
        autil::ScopedLock lock(_mutex);
        return tableStatus;
    }
    bool hasDeployingIncVersion() const { return deployMeta->hasDeployingIncVersion(); }

    void setDeployStatus(IncVersion incVersion, DeployStatus status) {
        deployMeta->setDeployStatus(incVersion, status);
    }
    IncVersion getLatestDeployVersion() const { return deployMeta->getLatestDeployVersion(); }
    DeployStatus getLatestDeployStatus() const { return deployMeta->getLatestDeployStatus(); }
    DeployStatus getDeployStatus(IncVersion incVersion) const { return deployMeta->getDeployStatus(incVersion); }

    std::set<IncVersion> getNeedKeepVersions() const { return deployMeta->getNeedKeepVersions(incVersion); }

    IncVersion getIncVersion() const {
        autil::ScopedLock lock(_mutex);
        return incVersion;
    }
    SchemaVersion getSchemaVersion() const {
        autil::ScopedLock lock(_mutex);
        return schemaVersion;
    }
    EffectiveFieldInfo getEffectiveFieldInfo() const {
        autil::ScopedLock lock(_mutex);
        return effectiveFieldInfo;
    }
    void setIncVersion(IncVersion incVersionId, SchemaVersion schemaVersionId = 0) {
        autil::ScopedLock lock(_mutex);
        incVersion = incVersionId;
        schemaVersion = schemaVersionId;
    }

    void setSchemaContent(std::string content) {
        autil::ScopedLock lock(_mutex);
        schemaContent = std::move(content);
    }

    void setRollbackTimestamp(int64_t rollbackTimestamp_) {
        autil::ScopedLock lock(_mutex);
        rollbackTimestamp = rollbackTimestamp_;
    }

    int64_t getRollbackTimestamp() const {
        autil::ScopedLock lock(_mutex);
        return rollbackTimestamp;
    }

    void setBranchId(uint64_t branchId_) {
        autil::ScopedLock lock(_mutex);
        branchId = branchId_;
    }

    uint64_t getBranchId() const {
        autil::ScopedLock lock(_mutex);
        return branchId;
    }

    const std::string &getSchemaContent() const {
        autil::ScopedLock lock(_mutex);
        return schemaContent;
    }

    void setEffectiveFieldInfo(const EffectiveFieldInfo &info) {
        autil::ScopedLock lock(_mutex);
        effectiveFieldInfo = info;
    }
    void clearEffectiveFieldInfo() {
        autil::ScopedLock lock(_mutex);
        effectiveFieldInfo.clear();
    }

    size_t getKeepCount() const {
        autil::ScopedLock lock(_mutex);
        return keepCount;
    }
    void setKeepCount(size_t keepCount_) {
        autil::ScopedLock lock(_mutex);
        keepCount = keepCount_;
    }

    size_t getConfigKeepCount() const {
        autil::ScopedLock lock(_mutex);
        return configKeepCount;
    }
    void setConfigKeepCount(size_t configKeepCount_) {
        autil::ScopedLock lock(_mutex);
        configKeepCount = configKeepCount_;
    }
    TableLoadType getTableLoadType() const {
        autil::ScopedLock lock(_mutex);
        return tableLoadType;
    }
    void setTableLoadType(TableLoadType tableLoadType_) {
        autil::ScopedLock lock(_mutex);
        tableLoadType = tableLoadType_;
    }
    std::string getConfigPath() const { return deployMeta->getConfigPath(); }
    void setConfigPath(const std::string &configPath_) { deployMeta->setConfigPath(configPath_); }
    void setIndexRoot(const std::string &indexRoot_) { deployMeta->setIndexRoot(indexRoot_); }
    std::string getIndexRoot() const { return deployMeta->getIndexRoot(); }
    void setRawIndexRoot(const std::string &rawIndexRoot_) { deployMeta->setRawIndexRoot(rawIndexRoot_); }
    std::string getRawIndexRoot() const { return deployMeta->getRawIndexRoot(); }

    void setTotalPartitionCount(int32_t count) {
        autil::ScopedLock lock(_mutex);
        tableMeta.totalPartitionCount = count;
    }

    int32_t getTotalPartitionCount() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.totalPartitionCount;
    }

    void setTableMode(TableMode mode) {
        autil::ScopedLock lock(_mutex);
        tableMeta.mode = mode;
    }

    TableMode getTableMode() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.mode;
    }

    SuezPartitionType getTableType() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta.tableType;
    }

    void setTableType(SuezPartitionType tableType) {
        autil::ScopedLock lock(_mutex);
        tableMeta.tableType = tableType;
    }

    void setTableMeta(const TableMeta &tableMeta) {
        autil::ScopedLock lock(_mutex);
        this->tableMeta = tableMeta;
    }

    const TableMeta &getTableMeta() const {
        autil::ScopedLock lock(_mutex);
        return tableMeta;
    }

    void setError(const SuezError &err) {
        autil::ScopedLock lock(_mutex);
        error = err;
    }

    void resetError(const SuezError &err) {
        autil::ScopedLock lock(_mutex);
        if (error == err) {
            error = ERROR_NONE;
        }
    }

    const SuezError &getError() const {
        autil::ScopedLock lock(_mutex);
        return error;
    }

    void setLoadedConfigPath(const std::string &loadedConfigPath_) {
        autil::ScopedLock lock(_mutex);
        loadedConfigPath = loadedConfigPath_;
    }

    const std::string &getLoadedConfigPath() const {
        autil::ScopedLock lock(_mutex);
        return loadedConfigPath;
    }

    void setLoadedIndexRoot(const std::string &loadedIndexRoot_) {
        autil::ScopedLock lock(_mutex);
        loadedIndexRoot = loadedIndexRoot_;
    }

    const std::string &getLoadedIndexRoot() const {
        autil::ScopedLock lock(_mutex);
        return loadedIndexRoot;
    }

    void setFullIndexLoaded(bool loaded) {
        autil::ScopedLock lock(_mutex);
        fullIndexLoaded = loaded;
    }

    bool getFullIndexLoaded() const {
        autil::ScopedLock lock(_mutex);
        return fullIndexLoaded;
    }

    void setRoleType(RoleType type) {
        autil::ScopedLock lock(_mutex);
        roleType = type;
    }

    RoleType getRoleType() const {
        autil::ScopedLock lock(_mutex);
        return roleType;
    }

    void clearIncVersion(const std::set<IncVersion> &inUseVersions);

    bool isTableInMemory() const {
        auto status = getTableStatus();
        return status == TS_LOADING || status == TS_PRELOADING || status == TS_UNLOADING || status == TS_FORCELOADING ||
               status == TS_LOADED;
    }

    const std::string &getCheckIndexPath() const {
        autil::ScopedLock lock(_mutex);
        return checkIndexPath;
    }

    /**
     * indicating table reaches target.
     */
    bool isReady(bool allowForceLoad) const {
        autil::ScopedLock lock(_mutex);
        if (tableMeta.tableType != SPT_INDEXLIB) {
            return (tableStatus == TS_LOADED);
        }
        if (!fullIndexLoaded) {
            return false;
        }
        return ((tableStatus == TS_LOADED && (allowForceLoad ? loadedConfigPath == getConfigPath() : true)) ||
                tableStatus == TS_PRELOADING || tableStatus == TS_PRELOAD_FAILED ||
                tableStatus == TS_PRELOAD_FORCE_RELOAD);
    }

    /**
     * indicating table is serving, IndexProvider can be gotten.
     * for index partition ONLY.
     */
    bool isAvailable(bool allowForceLoad, const PartitionMeta &target, const PartitionId &pid) const;

private:
    mutable autil::ThreadMutex _mutex;

    TableMeta tableMeta;
    std::shared_ptr<DeployMeta> deployMeta;
    // dp状态共享 避免role switch的时候 dp状态不同步
    IncVersion incVersion; // loaded incVersion
    SchemaVersion schemaVersion;
    int64_t rollbackTimestamp;
    uint64_t branchId;
    // indexlibv2回滚使用，给一个不同的branchId后会根据rollbackTimestamp和incVersion来决定回滚到那个版本
    EffectiveFieldInfo effectiveFieldInfo;
    size_t keepCount;
    size_t configKeepCount;
    TableLoadType tableLoadType;
    // 存储计算分离场景使用。在线需要等索引分发到在线盘古上version写出来。checkIndexPath就是分发完成的标记路径。
    std::string checkIndexPath;

    std::string loadedConfigPath;
    std::string loadedIndexRoot;
    std::string schemaContent;
    TableStatus tableStatus;
    SuezError error;
    RoleType roleType;
    bool fullIndexLoaded; // only for SuezIndexPartition
};

typedef std::shared_ptr<PartitionMeta> PartitionMetaPtr;

typedef PartitionMeta TargetPartitionMeta;
typedef PartitionMeta CurrentPartitionMeta;
typedef PartitionMetaPtr TargetPartitionMetaPtr;
typedef PartitionMetaPtr CurrentPartitionMetaPtr;

bool operator==(const DeployInfo &left, const DeployInfo &right);
bool operator!=(const DeployInfo &left, const DeployInfo &right);

bool operator==(const TableMeta &left, const TableMeta &right);
bool operator!=(const TableMeta &left, const TableMeta &right);
bool operator==(const PartitionMeta &left, const PartitionMeta &right);
bool operator!=(const PartitionMeta &left, const PartitionMeta &right);

class TableMetas : public std::map<PartitionId, PartitionMeta>, public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    void GetTableUserDefinedMetas(TableUserDefinedMetas &userDefinedMetas) const;
};

// for gtest
::std::ostream &operator<<(::std::ostream &os, const TableId &tableId);
::std::ostream &operator<<(::std::ostream &os, const TableMeta &tableMeta);
::std::ostream &operator<<(::std::ostream &os, const PartitionId &partitionId);
::std::ostream &operator<<(::std::ostream &os, const PartitionMeta &partitionMeta);
::std::ostream &operator<<(::std::ostream &os, const TableMetas &tableMetas);
::std::ostream &operator<<(::std::ostream &os, const DeployInfo &deployInfo);

} // namespace suez
