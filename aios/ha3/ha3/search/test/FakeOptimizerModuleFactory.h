#ifndef ISEARCH_FAKEOPTIMIZERMODULEFACTORY_H
#define ISEARCH_FAKEOPTIMIZERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerModuleFactory.h>

BEGIN_HA3_NAMESPACE(search);

class FakeOptimizer : public Optimizer {
public:
    FakeOptimizer() {}
    FakeOptimizer(const std::string &name)
        : _optimizerName(name)
    {}
    virtual ~FakeOptimizer() {}
public:
    bool init(OptimizeInitParam *param) {
        if (param->request == NULL) {
            return false;
        }
        return true;
    }
    OptimizerPtr clone() {
        return OptimizerPtr(new FakeOptimizer(*this));
    }
    std::string getName() const {
        return _optimizerName.empty() ? "FakeOptimizer" : _optimizerName;
    }
    bool optimize(OptimizeParam *param) {
        return true;
    }
    void disableTruncate() {
        return;
    }
private:
    std::string _optimizerName;
};

class FakeOptimizerModuleFactory : public OptimizerModuleFactory {
public:
    FakeOptimizerModuleFactory() {}
    virtual ~FakeOptimizerModuleFactory() {}
public:
    bool init(const KeyValueMap &parameters) {
        return true;
    }
    Optimizer* createOptimizer(const std::string &optimizerName, const OptimizerInitParam &param);

    void destroy() {
        delete this;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeOptimizerModuleFactory);
HA3_TYPEDEF_PTR(FakeOptimizer);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FAKEOPTIMIZERMODULEFACTORY_H

