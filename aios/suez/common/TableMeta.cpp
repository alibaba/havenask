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
#include "suez/common/TableMeta.h"

#include <assert.h>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableMeta);

namespace suez {

IncVersion DeployMeta::getLatestDeployVersion() const {
    autil::ScopedLock lock(_mutex);
    if (deployStatusMap.empty()) {
        return INVALID_VERSION;
    }
    return deployStatusMap.rbegin()->first;
}

DeployStatus DeployMeta::getLatestDeployStatus() const {
    autil::ScopedLock lock(_mutex);

    if (deployStatusMap.empty()) {
        return DS_UNKNOWN;
    }
    return deployStatusMap.rbegin()->second.deployStatus;
}

void DeployMeta::clearIncVersion(const set<IncVersion> &inUseVersions) {
    autil::ScopedLock lock(_mutex);
    vector<IncVersion> toEraseVersions;
    for (const auto &kv : deployStatusMap) {
        if (inUseVersions.find(kv.first) == inUseVersions.end()) {
            toEraseVersions.push_back(kv.first);
        }
    }
    AUTIL_LOG(DEBUG, "erase deployStatusMap versions [%s]", StringUtil::toString(toEraseVersions, ",").c_str());
    for (const auto &version : toEraseVersions) {
        deployStatusMap.erase(version);
    }
}

PartitionMeta &PartitionMeta::operator=(const PartitionMeta &other) {
    if (this == &other) {
        return *this;
    }
    autil::ScopedLock lock(other._mutex);
    tableMeta = other.tableMeta;
    deployMeta = other.deployMeta;
    tableStatus = other.tableStatus;
    incVersion = other.incVersion;
    schemaVersion = other.schemaVersion;
    rollbackTimestamp = other.rollbackTimestamp;
    branchId = other.branchId;
    effectiveFieldInfo = other.effectiveFieldInfo;
    keepCount = other.keepCount;
    configKeepCount = other.configKeepCount;
    tableLoadType = other.tableLoadType;
    checkIndexPath = other.checkIndexPath;
    loadedConfigPath = other.loadedConfigPath;
    loadedIndexRoot = other.loadedIndexRoot;
    roleType = other.roleType;
    fullIndexLoaded = other.fullIndexLoaded;
    error = other.error;
    tableDefConfig = other.tableDefConfig;
    return *this;
}

void PartitionMeta::Jsonize(JsonWrapper &json) {
    DeployStatus latestDeployStatus = getLatestDeployStatus();
    json.Jsonize("inc_version", incVersion, incVersion);
    json.Jsonize("schema_version", schemaVersion, schemaVersion);
    json.Jsonize("keep_count", keepCount, keepCount);
    json.Jsonize("config_keep_count", configKeepCount, configKeepCount);
    json.Jsonize("table_load_type", tableLoadType, tableLoadType);
    //*strongly prefer not use this rt_status in out json string*
    json.Jsonize("rt_status", tableMeta.rtStatus, tableMeta.rtStatus);
    json.Jsonize("check_index_path", checkIndexPath, checkIndexPath);
    json.Jsonize("role_type", roleType, roleType);
    json.Jsonize("rollback_timestamp", rollbackTimestamp, rollbackTimestamp);
    json.Jsonize("branch_id", branchId, branchId);

    if (json.GetMode() == TO_JSON) {
        json.Jsonize("table_status", tableStatus);
        json.Jsonize("deploy_status_map", deployMeta->getDeployStatusMap());
        json.Jsonize("deploy_status", latestDeployStatus);
        json.Jsonize("loaded_config_path", loadedConfigPath);
        json.Jsonize("loaded_index_root", loadedIndexRoot);
        if (INVALID_ERROR_CODE != error.errCode) {
            json.Jsonize("error_info", error, error);
        }
    } else {
        json.Jsonize("effective_field_info", effectiveFieldInfo, effectiveFieldInfo);
    }
}

bool PartitionMeta::isAvailable(bool allowForceLoad, const PartitionMeta &target, const PartitionId &pid) const {
    autil::ScopedLock lock(_mutex);
    if (!fullIndexLoaded) {
        AUTIL_LOG(DEBUG, "[%s]: full index has not been loaded yet.", FastToJsonString(pid, true).c_str());
        return false;
    }

    if (target.roleType != roleType) {
        return tableStatus != TS_ROLE_SWITCH_ERROR;
    }

    if (target.schemaVersion != 0 && target.schemaVersion > schemaVersion) {
        AUTIL_LOG(WARN,
                  "[%s]: target schema version gather than current: %ud vs %ud.",
                  FastToJsonString(pid, true).c_str(),
                  target.schemaVersion,
                  schemaVersion);
        return false;
    }

    if (!effectiveFieldInfo.contains(target.effectiveFieldInfo, pid)) {
        AUTIL_LOG(WARN, "[%s]: not all fields in target is ready.", FastToJsonString(pid, true).c_str());
        return false;
    }

    auto targetIncVersion = target.getIncVersion();
    if (targetIncVersion < incVersion) {
        AUTIL_LOG(WARN,
                  "[%s]: inc version revert from [%d] to [%d].",
                  FastToJsonString(pid, true).c_str(),
                  incVersion,
                  targetIncVersion);
        return false;
    }

    if (target.branchId != branchId) {
        AUTIL_LOG(INFO, "rollback, target branch id [%lu], current brnach id [%lu]", target.branchId, branchId);
        return false;
    }

    if (tableStatus == TS_LOADED) {
        if (allowForceLoad) {
            bool pathChanged = (loadedConfigPath != getConfigPath() || loadedIndexRoot != getIndexRoot());
            return !pathChanged;
        }
        return true;
    }

    return (tableStatus == TS_LOADING || tableStatus == TS_PRELOADING || tableStatus == TS_PRELOAD_FAILED ||
            tableStatus == TS_PRELOAD_FORCE_RELOAD || tableStatus == TS_ROLE_SWITCHING || tableStatus == TS_COMMITTING);
}

bool PartitionMeta::validate() const {
    return !getIndexRoot().empty() && !getConfigPath().empty() &&
           (tableMeta.mode == TM_READ_WRITE || INVALID_VERSION != incVersion);
}

void PartitionMeta::clearIncVersion(const set<IncVersion> &inUseVersions) {
    deployMeta->clearIncVersion(inUseVersions);
}

class JsonizePartMeta : public Jsonizable {
public:
    void Jsonize(Jsonizable::JsonWrapper &json) {
        tableMeta.Jsonize(json);
        json.Jsonize("partitions", partMetas);
    }

public:
    TableMeta tableMeta;
    map<string, PartitionMeta> partMetas;
};

void TableMetas::Jsonize(Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        map<string, map<string, JsonizePartMeta>> mp;
        std::map<TableId, TableMeta> tableMetas;

        for (auto it = this->begin(); it != this->end(); ++it) {
            const PartitionId &pid = it->first;
            const PartitionMeta &partMeta = it->second;
            JsonizePartMeta &jsonPartMeta = mp[pid.tableId.tableName][StringUtil::toString(pid.tableId.fullVersion)];
            jsonPartMeta.tableMeta = partMeta.getTableMeta();
            jsonPartMeta.tableMeta.configPath = partMeta.getConfigPath();
            jsonPartMeta.tableMeta.indexRoot = partMeta.getIndexRoot();
            jsonPartMeta.tableMeta.rawIndexRoot = partMeta.getRawIndexRoot();
            jsonPartMeta.partMetas[StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to)] = partMeta;
        }
        json = Jsonizable::JsonWrapper(ToJson(mp));
    } else {
        map<string, map<string, JsonizePartMeta>> mp;
        FromJson(mp, Any(json.GetMap()));
        for (auto it = mp.begin(); it != mp.end(); ++it) {
            const string &tableName = it->first;
            const map<string, JsonizePartMeta> &generations = it->second;
            for (auto sIt = generations.begin(); sIt != generations.end(); ++sIt) {
                const string &gidStr = sIt->first;
                const JsonizePartMeta &jsonPartMeta = sIt->second;
                TableId tableId;
                tableId.tableName = tableName;
                if (!StringUtil::fromString(gidStr, tableId.fullVersion)) {
                    throw ExceptionBase("Invalid fullversion string :" + gidStr);
                }

                for (auto mIt = jsonPartMeta.partMetas.begin(); mIt != jsonPartMeta.partMetas.end(); ++mIt) {
                    const string &rangeStr = mIt->first;
                    const PartitionMeta &partMeta = mIt->second;
                    PartitionId pid;
                    pid.tableId = tableId;
                    vector<string> ranges = StringTokenizer::tokenize(StringView(rangeStr), '_');
                    if (ranges.size() != 2 || !StringUtil::fromString(ranges[0], pid.from) ||
                        !StringUtil::fromString(ranges[1], pid.to)) {
                        throw ExceptionBase("Invalid range string :" + rangeStr);
                    }
                    PartitionMeta pm = partMeta;
                    const auto &tableMeta = jsonPartMeta.tableMeta;
                    pm.setTableMeta(jsonPartMeta.tableMeta);
                    pm.setConfigPath(tableMeta.configPath);
                    pm.setIndexRoot(tableMeta.indexRoot);
                    pm.setRawIndexRoot(tableMeta.rawIndexRoot);
                    if (pm.getTotalPartitionCount() > 0) {
                        pid.setPartitionIndex(pm.getTotalPartitionCount());
                    }
                    insert(make_pair(pid, pm));
                }
            }
        }
    }
}

void TableMetas::GetTableUserDefinedMetas(TableUserDefinedMetas &userDefinedMetas) const {
    for (auto it = this->begin(); it != this->end(); ++it) {
        const PartitionId &pid = it->first;
        const PartitionMeta &partMeta = it->second;
        userDefinedMetas[pid] = partMeta.getTableMeta().userDefinedParam;
    }
}

bool EffectiveFieldInfo::contains(const EffectiveFieldInfo &target, const PartitionId &pid) const {
    assert(std::is_sorted(attributes.begin(), attributes.end()));
    assert(std::is_sorted(indexes.begin(), indexes.end()));
    for (const auto &attr : target.attributes) {
        if (!std::binary_search(attributes.begin(), attributes.end(), attr)) {
            AUTIL_LOG(WARN,
                      "[%s]: can not find attribute %s in loaded table.",
                      FastToJsonString(pid, true).c_str(),
                      attr.c_str());
            return false;
        }
    }
    for (const auto &index : target.indexes) {
        if (!std::binary_search(indexes.begin(), indexes.end(), index)) {
            AUTIL_LOG(WARN,
                      "[%s]: can not find index %s in loaded table.",
                      FastToJsonString(pid, true).c_str(),
                      index.c_str());
            return false;
        }
    }
    return true;
}

#define LESS_COMPARATOR_HELPER(member)                                                                                 \
    if (left.member != right.member) {                                                                                 \
        return left.member < right.member;                                                                             \
    }

bool operator==(const DeployInfo &left, const DeployInfo &right) { return left.deployStatus == right.deployStatus; }

bool operator!=(const DeployInfo &left, const DeployInfo &right) { return !(left == right); }

bool operator==(const TableMeta &left, const TableMeta &right) {
    return left.mode == right.mode && left.tableType == right.tableType && left.rtStatus == right.rtStatus &&
           left.forceOnline == right.forceOnline && left.timestampToSkip == right.timestampToSkip;
}

bool operator!=(const TableMeta &left, const TableMeta &right) { return !(left == right); }

bool operator==(const PartitionMeta &left, const PartitionMeta &right) {
    return left.getTableMeta() == right.getTableMeta() && left.getIncVersion() == right.getIncVersion() &&
           left.getSchemaVersion() == right.getSchemaVersion() && left.getConfigPath() == right.getConfigPath() &&
           left.getIndexRoot() == right.getIndexRoot();
}

bool operator!=(const PartitionMeta &left, const PartitionMeta &right) { return !(left == right); }

::std::ostream &operator<<(::std::ostream &os, const TableMeta &tableMeta) {
    return os << FastToJsonString(tableMeta, true);
}

::std::ostream &operator<<(::std::ostream &os, const PartitionId &partitionId) {
    return os << FastToJsonString(partitionId, true);
}

::std::ostream &operator<<(::std::ostream &os, const PartitionMeta &partitionMeta) {
    return os << FastToJsonString(partitionMeta, true);
}

::std::ostream &operator<<(::std::ostream &os, const TableMetas &tableMetas) {
    return os << FastToJsonString(tableMetas, true);
}

::std::ostream &operator<<(::std::ostream &os, const DeployInfo &deployInfo) {
    return os << FastToJsonString(deployInfo, true);
}

#undef LESS_COMPARATOR_HELPER

} // namespace suez
