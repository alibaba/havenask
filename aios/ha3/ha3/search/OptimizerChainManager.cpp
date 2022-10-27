#include <ha3/search/OptimizerChainManager.h>
#include <ha3/search/AuxiliaryChainOptimizer.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, OptimizerChainManager);

OptimizerChainManager::OptimizerChainManager() {
}

OptimizerChainManager::~OptimizerChainManager() {
    //make sure _plugInManagerPtr destroy after _optimizers.
    _optimizers.clear();
    _plugInManagerPtr.reset();
}

bool OptimizerChainManager::addOptimizer(const OptimizerPtr &optimizerPtr) {
    assert(optimizerPtr);
    const string &optimizerName = optimizerPtr->getName();
    map<std::string, OptimizerPtr>::const_iterator iter = _optimizers.find(optimizerName);
    if (iter != _optimizers.end()) {
        HA3_LOG(ERROR, "duplicate optimizer name[%s]", optimizerName.c_str());
        return false;
    }
    _optimizers[optimizerName] = optimizerPtr;
    return true;
}

OptimizerChainPtr OptimizerChainManager::createOptimizerChain(const common::Request *request) const
{
    common::OptimizerClause *optimizerClause = request->getOptimizerClause();
    assert(optimizerClause);

    OptimizerChainPtr optimizerChainPtr(new OptimizerChain);
    uint32_t optimizeCount = optimizerClause->getOptimizeCount();
    for (uint32_t i = 0; i < optimizeCount; ++i) {
        const string &optimizeName = optimizerClause->getOptimizeName(i);
        map<string, OptimizerPtr>::const_iterator it = _optimizers.find(optimizeName);
        if (it == _optimizers.end()) {
            HA3_LOG(WARN, "Can not find optimizer[%s]", optimizeName.c_str());
            return OptimizerChainPtr();
        }
        const string &option = optimizerClause->getOptimizeOption(i);
        OptimizeInitParam param(option, request);
        OptimizerPtr optimizer = it->second->clone();
        if (!optimizer->init(&param)) {
            HA3_LOG(WARN, "Init optimizer[%s] failed", optimizeName.c_str());
            return OptimizerChainPtr();
        }
        optimizerChainPtr->addOptimizer(optimizer);
    }
    return optimizerChainPtr;
}

END_HA3_NAMESPACE(search);

