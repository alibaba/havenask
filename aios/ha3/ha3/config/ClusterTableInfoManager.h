#ifndef ISEARCH_CLUSTERTABLEINFOMANAGER_H
#define ISEARCH_CLUSTERTABLEINFOMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/common/JoinConfigInfo.h>

BEGIN_HA3_NAMESPACE(config);

class ClusterTableInfoManager
{
public:
    typedef std::pair<suez::turing::JoinConfigInfo, suez::turing::TableInfoPtr>
            JoinTableInfo;
    typedef std::map<std::string, JoinTableInfo> JoinTableInfos;
public:
    ClusterTableInfoManager();
    ~ClusterTableInfoManager();
private:
    ClusterTableInfoManager(const ClusterTableInfoManager &);
    ClusterTableInfoManager& operator=(const ClusterTableInfoManager &);
public:
    suez::turing::TableInfoPtr getClusterTableInfo() const;
    void setMainTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr) {
        _mainTableInfoPtr = tableInfoPtr;
    }
    const suez::turing::TableInfoPtr getMainTableInfo() {
        return _mainTableInfoPtr;
    }
    const JoinTableInfos& getJoinTableInfos() const {
        return _joinTableInfos;
    }
    void addJoinTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr,
	const suez::turing::JoinConfigInfo &joinConfigInfo);
    std::string getJoinFieldNameByTableName(const std::string &tableName) const;
    suez::turing::TableInfoPtr getJoinTableInfoByTableName(const std::string &tableName) const;
    const suez::turing::AttributeInfo *getAttributeInfo(const std::string &attributeName) const;
    void setScanJoinClusterName(const std::string &scanJoinClusterName);
    const std::string &getScanJoinClusterName() const;
public:
    static suez::turing::TableInfoPtr getClusterTableInfo(
            const suez::turing::TableInfoPtr &mainTableInfoPtr,
            const std::vector<suez::turing::TableInfoPtr> &auxTableInfos);
private:
    suez::turing::TableInfoPtr _mainTableInfoPtr;
    JoinTableInfos _joinTableInfos;
    std::string _scanJoinClusterName;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ClusterTableInfoManager);

class ClusterTableInfoManagerMap : public std::map<std::string, ClusterTableInfoManagerPtr> {
public:
    ClusterTableInfoManagerPtr getClusterTableInfoManager(const std::string &clusterName) const;
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(ClusterTableInfoManagerMap);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_CLUSTERTABLEINFOMANAGER_H
