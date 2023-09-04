#pragma once
#include <string>
#include <memory>
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/engine/NaviUserResult.h"

namespace navi {

class NaviGraphRunner {
public:
    NaviGraphRunner();
    NaviGraphRunner(const NaviGraphRunner &) = delete;
    NaviGraphRunner &operator=(const NaviGraphRunner &) = delete;
public:
    bool init();
    NaviUserResultPtr runLocalGraph(GraphDef *graphDef,
                                    const ResourceMap &resourceMap,
                                    const std::string &taskQueueName = "",
                                    int64_t timeoutMs = 2000000);
    void runLocalGraphAsync(GraphDef *graphDef,
                            const ResourceMap &resourceMap,
                            NaviUserResultClosure *closure,
                            const std::string &taskQueueName = "",
                            int64_t timeoutMs = 2000000);
    std::string getBizName() const;
private:
    bool addBiz(const std::string &host,
                const std::string &biz,
                NaviPartId partCount,
                NaviPartId partId);
    std::string getConfigPath() const;
    std::string getBizConfig() const;
private:
    std::string _loader;
    std::string _configName;
    std::string _bizConfig;
    ResourceMapPtr _rootResourceMap;
    std::unique_ptr<NaviTestCluster> _cluster;
    NaviPtr _navi;
};

}
