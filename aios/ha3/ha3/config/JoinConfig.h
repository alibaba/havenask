#ifndef ISEARCH_JOINCONFIG_H
#define ISEARCH_JOINCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/TypeDefine.h>
#include <suez/turing/common/JoinConfigInfo.h>

BEGIN_HA3_NAMESPACE(config);

class JoinConfig : public autil::legacy::Jsonizable
{
public:
    JoinConfig();
    ~JoinConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);    
public:
    bool empty() const { return _joinConfigInfos.empty(); }

    void getJoinClusters(std::vector<std::string> &clusterNames) const;
    void getJoinFields(std::vector<std::string> &joinFields) const;
    const std::vector<suez::turing::JoinConfigInfo> &getJoinInfos() const {
        return _joinConfigInfos;
    }
    std::vector<suez::turing::JoinConfigInfo> &getJoinInfos() {
        return _joinConfigInfos;
    }
    void addJoinInfo(const suez::turing::JoinConfigInfo &joinInfo) {
        _joinConfigInfos.push_back(joinInfo);
    }
    void setScanJoinCluster(const std::string &scanJoinCluster) {
	_scanJoinCluster = scanJoinCluster;
    }
    const std::string &getScanJoinCluster() const { return _scanJoinCluster; }
    bool operator==(const JoinConfig &other) const {
        return _joinConfigInfos == other._joinConfigInfos;
    }
    bool operator!=(const JoinConfig &other) const {
        return !(*this == other);
    }    
private:
    void doCompatibleWithOldFormat(autil::legacy::Jsonizable::JsonWrapper& json);
private:
    std::string _scanJoinCluster;
    std::vector<suez::turing::JoinConfigInfo> _joinConfigInfos;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JoinConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_JOINCONFIG_H
