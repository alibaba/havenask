#ifndef ISEARCH_OPTIMIZECHAINMANAGER_H
#define ISEARCH_OPTIMIZECHAINMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerChain.h>
#include <ha3/common/OptimizerClause.h>
#include <build_service/plugin/PlugInManager.h>

BEGIN_HA3_NAMESPACE(search);

class OptimizerChainManager
{
public:
    OptimizerChainManager();
    ~OptimizerChainManager();
private:
    OptimizerChainManager(const OptimizerChainManager &);
    OptimizerChainManager& operator=(const OptimizerChainManager &);
public:
    OptimizerChainPtr createOptimizerChain(const common::Request *request) const;
    void setPlugInManager(const build_service::plugin::PlugInManagerPtr &plugInManagerPtr) {
        _plugInManagerPtr = plugInManagerPtr;
    }
    bool addOptimizer(const OptimizerPtr &optimizerPtr);
private:
    std::map<std::string, OptimizerPtr> _optimizers;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
private:
    HA3_LOG_DECLARE();
    friend class OptimizerChainManagerCreatorTest;
    friend class OptimizerChainManagerTest;    
};

HA3_TYPEDEF_PTR(OptimizerChainManager);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZECHAINMANAGER_H
