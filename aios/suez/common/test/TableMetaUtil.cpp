#include "suez/common/test/TableMetaUtil.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace suez {

PartitionId TableMetaUtil::makePidFromStr(const string &str) {
    auto strVec = StringUtil::split(str, "_");
    assert(!strVec.empty());
    PartitionId pid;
    pid.tableId.tableName = strVec[0];
    pid.tableId.fullVersion = 0;
    if (strVec.size() > 1) {
        pid.tableId.fullVersion = StringUtil::fromString<FullVersion>(strVec[1]);
    }
    pid.from = 0;
    if (strVec.size() > 2) {
        pid.from = StringUtil::fromString<RangeType>(strVec[2]);
    }
    pid.to = 65535;
    if (strVec.size() > 3) {
        pid.to = StringUtil::fromString<RangeType>(strVec[3]);
    }
    return pid;
}

PartitionMeta TableMetaUtil::makeTargetFromStr(const string &str) {
    PartitionMeta meta;
    auto strVec = StringUtil::split(str, "_");
    assert(!strVec.empty());
    meta.setIncVersion(StringUtil::fromString<IncVersion>(strVec[0]));
    if (strVec.size() > 1) {
        meta.setConfigPath(strVec[1]);
    }
    if (strVec.size() > 2) {
        meta.setIndexRoot(strVec[2]);
    }
    SuezPartitionType type = SPT_INDEXLIB;
    if (strVec.size() > 3) {
        type = strVec[3] == "indexlib" ? SPT_INDEXLIB : (strVec[3] == "rawfile" ? SPT_RAWFILE : SPT_NONE);
    }
    meta.setTableType(type);
    if (strVec.size() > 4) {
        uint32_t keepCount = StringUtil::fromString<uint32_t>(strVec[4]);
        meta.setKeepCount(keepCount);
        meta.setConfigKeepCount(keepCount);
    }

    if (strVec.size() > 5) {
        TableMode tableMode;
        tableMode = strVec[5] == "rw" ? TM_READ_WRITE : TM_NORMAL;
        meta.setTableMode(tableMode);
    }

    if (strVec.size() > 6) {
        uint32_t configKeepCount = StringUtil::fromString<uint32_t>(strVec[6]);
        meta.setConfigKeepCount(configKeepCount);
    }
    return meta;
}

std::pair<PartitionId, PartitionMeta> TableMetaUtil::makeTableMeta(const std::string &pidStr,
                                                                   const std::string &targetStr) {
    auto pid = makePidFromStr(pidStr);
    auto meta = makeTargetFromStr(targetStr);
    return std::make_pair(pid, meta);
}

TableMetas TableMetaUtil::makeTableMetas(const std::vector<std::string> &pidStrVec,
                                         const std::vector<std::string> &targetStrVec) {
    TableMetas tableMetas;

    assert(pidStrVec.size() == targetStrVec.size());
    for (size_t i = 0; i < pidStrVec.size(); i++) {
        tableMetas.insert(TableMetaUtil::makeTableMeta(pidStrVec[i], targetStrVec[i]));
    }

    return tableMetas;
}

PartitionId TableMetaUtil::makePid(const std::string &name, FullVersion fullVersion, RangeType from, RangeType to) {
    PartitionId pid;
    pid.tableId.tableName = name;
    pid.tableId.fullVersion = fullVersion;
    pid.from = from;
    pid.to = to;
    return pid;
}

PartitionMeta TableMetaUtil::makeLoaded(int incVersion) { return make(incVersion, TS_LOADED, DS_DEPLOYDONE); }

PartitionMeta TableMetaUtil::makeTarget(IncVersion incVersion,
                                        const string &indexRoot,
                                        const string &configPath,
                                        SuezPartitionType tableType) {
    PartitionMeta meta;
    meta.setIncVersion(incVersion);
    meta.setConfigPath(configPath);
    meta.setIndexRoot(indexRoot);
    meta.setTableType(tableType);
    return meta;
}

PartitionMeta TableMetaUtil::make(int incVersion,
                                  TableStatus tableStatus,
                                  DeployStatus deployStatus,
                                  const std::string &indexRoot,
                                  const std::string &configPath) {
    PartitionMeta partMeta;
    partMeta.setIncVersion(incVersion);
    partMeta.setIndexRoot(indexRoot);
    partMeta.setLoadedIndexRoot(indexRoot);
    partMeta.setConfigPath(configPath);
    partMeta.setLoadedConfigPath(configPath);
    partMeta.setTableStatus(tableStatus);
    // partMeta.setTableType(SPT_INDEXLIB);
    if (incVersion != -1) {
        partMeta.setDeployStatus(incVersion, deployStatus);
    }
    return partMeta;
}

PartitionMeta TableMetaUtil::makeWithTableType(int incVersion,
                                               TableStatus tableStatus,
                                               DeployStatus deployStatus,
                                               SuezPartitionType tableType,
                                               bool fullIndexLoaded,
                                               const std::string &indexRoot,
                                               const std::string &configPath) {
    PartitionMeta partMeta;
    partMeta.setIncVersion(incVersion);
    partMeta.setIndexRoot(indexRoot);
    partMeta.setLoadedIndexRoot(indexRoot);
    partMeta.setConfigPath(configPath);
    partMeta.setLoadedConfigPath(configPath);
    partMeta.setTableStatus(tableStatus);
    partMeta.setTableType(tableType);
    partMeta.setFullIndexLoaded(fullIndexLoaded);
    if (incVersion != -1) {
        partMeta.setDeployStatus(incVersion, deployStatus);
    }
    return partMeta;
}

PartitionMeta TableMetaUtil::fromString(const std::string &partMetasStr, bool indexLoaded) {
    vector<string> partMetasStrs = StringTokenizer::tokenize(StringView(partMetasStr), "_");
    PartitionMeta partMeta;
    partMeta.setFullIndexLoaded(indexLoaded);
    partMeta.setTableType(SPT_INDEXLIB);
    partMeta.setIncVersion(StringUtil::fromString<int>(partMetasStrs[0]));
    if (partMetasStrs.size() > 1) {
        partMeta.setTableStatus(strToTableStatus(partMetasStrs[1]));
        int latestDeployedInc = partMeta.getIncVersion();
        for (int i = 1; i <= latestDeployedInc; ++i) {
            partMeta.setDeployStatus(i, DS_DEPLOYDONE);
        }
    }
    return partMeta;
}

TableMetas TableMetaUtil::constructTableMetas(const std::string &tableMetasStr) {
    vector<string> partMetasStrs = StringTokenizer::tokenize(StringView(tableMetasStr), ",");
    TableMetas tableMetas;
    for (size_t i = 0; i < partMetasStrs.size(); ++i) {
        vector<string> partMeta = StringTokenizer::tokenize(StringView(partMetasStrs[i]), ":");
        assert(partMeta.size() == 2);
        tableMetas[makePid(partMeta[0])] = fromString(partMeta[1]);
    }
    return tableMetas;
}

TableStatus TableMetaUtil::strToTableStatus(const string &tableStatusStr) {
    if (tableStatusStr == "INITIALIZING") {
        return TS_INITIALIZING;
    } else if (tableStatusStr == "UNLOADED") {
        return TS_UNLOADED;
    } else if (tableStatusStr == "LOADING") {
        return TS_LOADING;
    } else if (tableStatusStr == "UNLOADING") {
        return TS_UNLOADING;
    } else if (tableStatusStr == "FORCELOADING") {
        return TS_FORCELOADING;
    } else if (tableStatusStr == "LOADED") {
        return TS_LOADED;
    } else if (tableStatusStr == "FORCERELOAD") {
        return TS_FORCE_RELOAD;
    } else if (tableStatusStr == "ERRORLACKMEM") {
        return TS_ERROR_LACK_MEM;
    } else if (tableStatusStr == "PRELOADFAILED") {
        return TS_PRELOAD_FAILED;
    } else if (tableStatusStr == "PRELOADFORCERELOAD") {
        return TS_PRELOAD_FORCE_RELOAD;
    } else if (tableStatusStr == "PRELOADING") {
        return TS_PRELOADING;
    } else if (tableStatusStr == "ERRORCONFIG") {
        return TS_ERROR_CONFIG;
    } else if (tableStatusStr == "UNKNOWN") {
        return TS_ERROR_UNKNOWN;
    } else {
        assert(false);
        return TS_UNKNOWN;
    }
}

} // namespace suez
