#include <ha3/search/OptimizerChain.h>

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, OptimizerChain);

OptimizerChain::OptimizerChain() {
}

OptimizerChain::~OptimizerChain() {
}

void OptimizerChain::addOptimizer(const OptimizerPtr &optimizer) {
    _optimizers.push_back(optimizer);
}

bool OptimizerChain::optimize(OptimizeParam *param)
{
    const Request *request = param->request;
    bool useTruncate = request->getConfigClause()->useTruncateOptimizer();
    for (std::vector<OptimizerPtr>::const_iterator it = _optimizers.begin();
         it != _optimizers.end(); ++it)
    {
        const OptimizerPtr &optimzier = *it;
        if (!useTruncate) {
            optimzier->disableTruncate();
        }
        if (!optimzier->optimize(param)) {
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(search);

