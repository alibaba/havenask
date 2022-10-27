#ifndef ISEARCH_OPTIMIZECHAIN_H
#define ISEARCH_OPTIMIZECHAIN_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Optimizer.h>
#include <ha3/monitor/SessionMetricsCollector.h>

BEGIN_HA3_NAMESPACE(search);

class OptimizerChain
{
public:
    OptimizerChain();
    ~OptimizerChain();
private:
    OptimizerChain(const OptimizerChain &);
    OptimizerChain& operator=(const OptimizerChain &);
public:
    void addOptimizer(const OptimizerPtr &optimizer);
    bool optimize(OptimizeParam *param);
private:
    std::vector<OptimizerPtr> _optimizers;
private:
    HA3_LOG_DECLARE();
private:
    friend class OptimizerChainManagerCreatorTest;
    friend class OptimizerChainManagerTest;
};

HA3_TYPEDEF_PTR(OptimizerChain);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZECHAIN_H
