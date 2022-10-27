#ifndef ISEARCH_CLUSTERCONFIGINFO_H
#define ISEARCH_CLUSTERCONFIGINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/QueryInfo.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/config/JoinConfig.h>
#include <ha3/config/ResourceTypeSet.h>
#include <build_service/config/HashMode.h>
#include <indexlib/indexlib.h>
#include <autil/HashFunctionBase.h>

BEGIN_HA3_NAMESPACE(config);
typedef build_service::config::HashMode HashMode;

class ClusterConfigInfo : public autil::legacy::Jsonizable
{
public:
    ClusterConfigInfo();
    ~ClusterConfigInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    void setTableName(const std::string &tableName) {
        _tableName = tableName;
    }
    const std::string& getTableName() const {
        return _tableName;
    }
    const HashMode& getHashMode() const {
        return _hashMode;
    }
    void setQueryInfo(const QueryInfo& queryInfo) {
        _queryInfo = queryInfo;
    }
    const QueryInfo& getQueryInfo() const {
        return _queryInfo;
    }

    const JoinConfig& getJoinConfig() const {
        return _joinConfig;
    }
    autil::HashFunctionBasePtr getHashFunction() const {
        return _hashFunctionBasePtr;
    }
    bool initHashFunc();
    bool check() const;
public:
    std::string _tableName;
    HashMode _hashMode;
    autil::HashFunctionBasePtr _hashFunctionBasePtr;
    config::QueryInfo _queryInfo;
    JoinConfig _joinConfig;
    uint32_t _returnHitThreshold;
    double _returnHitRatio;
    size_t _poolTrunkSize; // M
    size_t _poolRecycleSizeLimit; // M
    size_t _poolMaxCount;
    size_t _threadInitRoundRobin;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ClusterConfigInfo);

typedef std::map<std::string, ClusterConfigInfo> ClusterConfigMap;
HA3_TYPEDEF_PTR(ClusterConfigMap);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_CLUSTERCONFIGINFO_H
