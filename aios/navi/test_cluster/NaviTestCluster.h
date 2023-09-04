#ifndef NAVI_NAVITESTCLUSTER_H
#define NAVI_NAVITESTCLUSTER_H

#include "navi/common.h"
#include "navi/engine/Navi.h"
#include "navi/test_cluster/NaviTestServer.h"
#include <set>

namespace navi {

class NaviTestServer;

class NaviTestCluster
{
public:
    NaviTestCluster();
    ~NaviTestCluster();
private:
    NaviTestCluster(const NaviTestCluster &);
    NaviTestCluster &operator=(const NaviTestCluster &);
public:
    bool addServer(const std::string &serverName,
                   const std::string &loader,
                   const std::string &configPath,
                   const ResourceMapPtr &rootResourceMap);
    bool addBiz(const std::string &serverName,
                const std::string &bizName,
                NaviPartId partCount,
                NaviPartId partId,
                const std::string &bizConfig);
    bool start();
    NaviPtr getNavi(const std::string &serverName) const;
    std::vector<std::string> getBizList() const;
    multi_call::LocalConfig getClusterGigConfig() const;
    void addGigConfig(const multi_call::LocalConfig &config);
private:
    NaviTestServer *getServer(const std::string &serverName) const;
    std::string getClusterGigConfigStr() const;
private:
    std::unordered_map<std::string, NaviTestServer *> _servers;
    std::set<std::string> _bizSet;
    bool _started = false;
    multi_call::LocalConfig _externalConfig;
};

}

#endif //NAVI_NAVITESTCLUSTER_H
