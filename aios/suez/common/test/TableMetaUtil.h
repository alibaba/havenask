#pragma once

#include "suez/common/TableMeta.h"

namespace suez {

class TableMetaUtil {
public:
    // tableName_fullVersion_from_to
    static PartitionId makePidFromStr(const std::string &str);
    // incVersion_indexRoot_configPath_tableType_keepCount_...
    static PartitionMeta makeTargetFromStr(const std::string &str);

    static std::pair<PartitionId, PartitionMeta> makeTableMeta(const std::string &pidStr, const std::string &targetStr);

    static TableMetas makeTableMetas(const std::vector<std::string> &pidStrVec,
                                     const std::vector<std::string> &targetStrVec);

    static PartitionId
    makePid(const std::string &name, FullVersion fullVersion = 0, RangeType from = 0, RangeType to = 65535);

    static PartitionMeta makeLoaded(int incVersion);
    static PartitionMeta makeTarget(IncVersion incVersion,
                                    const std::string &indexRoot = "",
                                    const std::string &configPath = "",
                                    SuezPartitionType tableType = SPT_INDEXLIB);
    static PartitionMeta make(int incVersion,
                              TableStatus tableStatus = TS_UNKNOWN,
                              DeployStatus deployStatus = DS_UNKNOWN,
                              const std::string &indexRoot = "",
                              const std::string &configPath = "fake_config/1483473855");

    static PartitionMeta makeWithTableType(int incVersion,
                                           TableStatus tableStatus = TS_UNKNOWN,
                                           DeployStatus deployStatus = DS_UNKNOWN,
                                           SuezPartitionType = SPT_INDEXLIB,
                                           bool fullIndexLoaded = false,
                                           const std::string &indexRoot = "",
                                           const std::string &configPath = "fake_config/1483473855");

    // 1_LOADING
    static PartitionMeta fromString(const std::string &partMetasStr, bool indexLoaded = true);

    // table_name_1: 1_LOADING,table_name_2: 44_LOADED
    static TableMetas constructTableMetas(const std::string &tableMetasStr);

private:
    static TableStatus strToTableStatus(const std::string &tableStatusStr);
};

} // namespace suez
