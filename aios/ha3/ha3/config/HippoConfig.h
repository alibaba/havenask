#ifndef ISEARCH_HIPPOCONFIG_H
#define ISEARCH_HIPPOCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <autil/StringUtil.h>
#include <hippo/DriverCommon.h>

namespace hippo {
bool operator<(const DataInfo &lhs, const DataInfo &rhs);
bool operator==(const DataInfo &lhs, const DataInfo &rhs);
}

BEGIN_HA3_NAMESPACE(config);

struct CM2ServerConfig : public autil::legacy::Jsonizable {
public:
    CM2ServerConfig()
        : idcType(0)
        , weight(100)
    {
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("cm2_server_zookeeper_host", zkHost, "");
        json.Jsonize("cm2_server_leader_path", leaderPath, "");
        json.Jsonize("cm2_server_cluster_name", clusterName, "");
        json.Jsonize("signature", signature, "");
        json.Jsonize("idc_type", idcType, 0);
        json.Jsonize("weight", weight, 100);
    }
public:
    std::string zkHost;
    std::string leaderPath;
    std::string clusterName;
    std::string signature;
    int32_t idcType;
    int32_t weight;
};

struct RoleDefine : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("processInfos", processInfos, processInfos);
    }
public:
    std::vector<hippo::ProcessInfo> processInfos;
};

typedef hippo::DataInfo RequiredData;

typedef hippo::RoleRequest ResourceDefine;

struct HippoConfig : public autil::legacy::Jsonizable {
public:
    HippoConfig();
    ~HippoConfig();
public:    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
private:
    void JsonizeLegacyPackage(autil::legacy::Jsonizable::JsonWrapper& json);
    void JsonizeLegacyRequireDatas(autil::legacy::Jsonizable::JsonWrapper& json);
    void JsonizeLegacyEnvs(autil::legacy::Jsonizable::JsonWrapper& json);
    void JsonizeLegacyArgs(autil::legacy::Jsonizable::JsonWrapper& json);
    void JsonizeLegacyResources(autil::legacy::Jsonizable::JsonWrapper& json);
    void JsonizeLegacyExtraRoleProcesses(autil::legacy::Jsonizable::JsonWrapper& json);
public:
    std::string hippoZookeeperRoot;
    std::string appName;
    std::string user;
    CM2ServerConfig cm2ServerConfig;
    std::vector<hippo::PackageInfo> packages;
    std::vector<RequiredData> requiredDatas;
    std::map<std::string, ResourceDefine> resourceDefines;
    RoleDefine roleDefaultDefine;
    std::map<std::string, RoleDefine> roleCustomizeDefines;
};

HA3_TYPEDEF_PTR(HippoConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_HIPPOCONFIG_H
