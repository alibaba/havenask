#include <ha3/config/HippoConfig.h>

namespace hippo {
bool operator<(const DataInfo &lhs, const DataInfo &rhs) {
    return lhs.name < rhs.name;
}
bool operator==(const DataInfo &lhs, const DataInfo &rhs) {
    return lhs.name == rhs.name
        && lhs.src == rhs.src
        && lhs.dst == rhs.dst;
}
}

using namespace std;
using namespace hippo;

BEGIN_HA3_NAMESPACE(config);

HippoConfig::HippoConfig() {
    roleDefaultDefine.processInfos.resize(1);
}

HippoConfig::~HippoConfig() {
}

void HippoConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("hippo_zookeeper_root", hippoZookeeperRoot, "");
    json.Jsonize("app_name", appName, "");
    json.Jsonize("user", user, "");
    json.Jsonize("cm2_server", cm2ServerConfig, cm2ServerConfig);
    json.Jsonize("packages", packages, packages);
    try {
        json.Jsonize("required_datas", requiredDatas, requiredDatas);
    } catch (const autil::legacy::NotJsonizableException &e) {
    }
    json.Jsonize("resource_defines", resourceDefines, resourceDefines);
    json.Jsonize("role_default", roleDefaultDefine, roleDefaultDefine);
    json.Jsonize("role_customize", roleCustomizeDefines, roleCustomizeDefines);
    if (FROM_JSON == json.GetMode()) {
        if (roleDefaultDefine.processInfos.empty()) {
            roleDefaultDefine.processInfos.resize(1);
        }
        roleDefaultDefine.processInfos[0].isDaemon = true;
        JsonizeLegacyPackage(json);
        JsonizeLegacyRequireDatas(json);
        JsonizeLegacyEnvs(json);
        JsonizeLegacyArgs(json);
        JsonizeLegacyExtraRoleProcesses(json);
    }
}

struct LegacyPackage : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("type", type, type);
        json.Jsonize("uri", uri, uri);
    }
public:
    std::string type;
    std::string uri;
};

void HippoConfig::JsonizeLegacyPackage(autil::legacy::Jsonizable::JsonWrapper& json) {
    vector<LegacyPackage> legacyPackages;
    json.Jsonize("packages", legacyPackages, legacyPackages);
    assert(legacyPackages.size() == packages.size());
    for (size_t i = 0; i < packages.size(); i++) {
        if (!legacyPackages[i].uri.empty()) {
            packages[i].URI = legacyPackages[i].uri;
        }
    }
}

struct LegacyRequiredData : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("name", name, name);
        json.Jsonize("source", source, source);
        json.Jsonize("dest", dest, dest);
    }
public:
    std::string name;
    std::string source;
    std::string dest;
};

void HippoConfig::JsonizeLegacyRequireDatas(autil::legacy::Jsonizable::JsonWrapper& json) {
    vector<LegacyRequiredData> legacyRequiredDatas;
    json.Jsonize("required_datas", legacyRequiredDatas, legacyRequiredDatas);
    for (size_t i = 0; i < legacyRequiredDatas.size(); i++) {
        if (legacyRequiredDatas[i].source.empty() ||
            legacyRequiredDatas[i].dest.empty())
        {
            continue;
        }
        RequiredData dataInfo;
        dataInfo.name = legacyRequiredDatas[i].name;
        dataInfo.src = legacyRequiredDatas[i].source;
        dataInfo.dst = legacyRequiredDatas[i].dest;
        requiredDatas.push_back(dataInfo);
    }
}
void HippoConfig::JsonizeLegacyEnvs(autil::legacy::Jsonizable::JsonWrapper& json) {
    string hadoopHome;
    json.Jsonize("hadoop_home", hadoopHome, "");
    string externLibPath;
    json.Jsonize("extern_lib_path", externLibPath, "");
    string javaHome;
    json.Jsonize("java_home", javaHome, "");
    string classpath;
    json.Jsonize("classpath", classpath, "");
    string preloadPath;
    json.Jsonize("preload_path", preloadPath, "");

    if (!hadoopHome.empty()) {
        roleDefaultDefine.processInfos[0].envs.push_back(
                make_pair("HADOOP_HOME", hadoopHome));
    }
    if (!javaHome.empty()) {
        roleDefaultDefine.processInfos[0].envs.push_back(
                make_pair("JAVA_HOME", javaHome));
        externLibPath += ":$JAVA_HOME/jre/lib/amd64/server/";
    }
    if (!externLibPath.empty()) {
        roleDefaultDefine.processInfos[0].envs.push_back(
                make_pair("LD_LIBRARY_PATH", externLibPath));
    }
    if (!classpath.empty()) {
        roleDefaultDefine.processInfos[0].envs.push_back(
                make_pair("CLASSPATH", classpath));
    }
    if (!preloadPath.empty()) {
        roleDefaultDefine.processInfos[0].envs.push_back(
                make_pair("LD_PRELOAD", preloadPath));
    }
}

void HippoConfig::JsonizeLegacyArgs(autil::legacy::Jsonizable::JsonWrapper& json) {
    string logConf;
    json.Jsonize("log_conf", logConf, "");
    if (!logConf.empty()) {
        roleDefaultDefine.processInfos[0].args.push_back(make_pair("--logConf", logConf));
    } else {
        bool found = false;
        for (size_t i = 0; i < roleDefaultDefine.processInfos[0].args.size(); i++) {
            if (roleDefaultDefine.processInfos[0].args[i].first == "--logConf") {
                found = true;
                break;
            }
        }
        if (!found) {
            roleDefaultDefine.processInfos[0].args.push_back(make_pair("--logConf",
                            "$HIPPO_APP_INST_ROOT/usr/local/etc/ha3/ha3_alog.conf"));
        }
    }
}

struct LegacyRoleProcessInfo : public autil::legacy::Jsonizable
{
public:
    LegacyRoleProcessInfo() {
        isDaemon = true;
    }
    ~LegacyRoleProcessInfo() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("isDaemon", isDaemon, isDaemon);
        json.Jsonize("processName", processName, processName);
        json.Jsonize("cmd", cmd, cmd);
        json.Jsonize("envs", envs, envs);
    }
public:
    bool isDaemon;
    std::string processName;
    std::string cmd;
    std::map<std::string, std::string> envs;
};

void HippoConfig::JsonizeLegacyExtraRoleProcesses(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    typedef std::map<std::string, std::vector<LegacyRoleProcessInfo> > ExtraRoleProcesses;
    ExtraRoleProcesses extraRoleProcesses;
    json.Jsonize("extra_role_processes", extraRoleProcesses, extraRoleProcesses);
    for (ExtraRoleProcesses::const_iterator it = extraRoleProcesses.begin();
         it != extraRoleProcesses.end(); ++it)
    {
        for (size_t i = 0; i < it->second.size(); i++) {
            hippo::ProcessInfo processInfo;
            processInfo.isDaemon = it->second[i].isDaemon;
            processInfo.name = it->second[i].processName;
            processInfo.cmd = it->second[i].cmd;
            for (map<string, string>::const_iterator envIt = it->second[i].envs.begin();
                 envIt != it->second[i].envs.end(); ++envIt)
            {
                processInfo.envs.push_back(make_pair(envIt->first, envIt->second));
            }
            roleCustomizeDefines[it->first].processInfos.push_back(processInfo);
        }
    }
}

END_HA3_NAMESPACE(config);
