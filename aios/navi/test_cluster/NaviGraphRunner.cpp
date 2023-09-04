#include "navi/test_cluster/NaviGraphRunner.h"
#include "navi/engine/NaviSnapshot.h"
#include "autil/EnvUtil.h"

using namespace autil;

namespace navi {

NaviGraphRunner::NaviGraphRunner() {}

bool NaviGraphRunner::init() {
    EnvGuard _("NAVI_PYTHON_HOME", "./aios/navi/config_loader/python:/usr/lib64/python3.6/");
    _loader = "./aios/navi/testdata/test_config_loader.py";
    _configName = "run_graph_test";
    _bizConfig = "biz1.py";
    _rootResourceMap.reset(new ResourceMap());
    _cluster.reset(new NaviTestCluster());

    if (!addBiz("host_0", getBizName(), 1, 0)) {
        return false;
    }
    if (!_cluster->start()) {
        return false;
    }
    _navi = _cluster->getNavi("host_0");
    return true;
}

NaviUserResultPtr NaviGraphRunner::runLocalGraph(GraphDef *graphDef,
                                                 const ResourceMap &resourceMap,
                                                 const std::string &taskQueueName,
                                                 int64_t timeoutMs) {
    assert(_navi);
    RunGraphParams params;
    params.setTimeoutMs(timeoutMs);
    params.setTaskQueueName(taskQueueName);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);
    return _navi->runLocalGraph(graphDef, params, resourceMap);
}

void NaviGraphRunner::runLocalGraphAsync(GraphDef *graphDef,
                                         const ResourceMap &resourceMap,
                                         NaviUserResultClosure *closure,
                                         const std::string &taskQueueName,
                                         int64_t timeoutMs) {
    assert(_navi);
    RunGraphParams params;
    params.setTimeoutMs(timeoutMs);
    params.setTaskQueueName(taskQueueName);
    params.setTraceLevel("ERROR");
    params.setCollectMetric(true);
    return _navi->runLocalGraphAsync(graphDef, params, resourceMap, closure);
}

std::string NaviGraphRunner::getBizName() const {
    return "graph_runner_main_biz";
}

bool NaviGraphRunner::addBiz(const std::string &host,
                             const std::string &biz,
                             NaviPartId partCount,
                             NaviPartId partId) {
    if (!_cluster->addServer(host, _loader, getConfigPath(), _rootResourceMap)) {
        return false;
    }
    if (!_cluster->addBiz(host, biz, partCount, partId, getBizConfig())) {
        return false;
    }
    return true;
}

std::string NaviGraphRunner::getConfigPath() const {
    return "./aios/navi/testdata/config/" + _configName;
}

std::string NaviGraphRunner::getBizConfig() const {
    return getConfigPath() + "/" + _bizConfig;
}

}
