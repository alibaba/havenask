#ifndef ISEARCH_OPTIMIZERMODULEFACTORY_H
#define ISEARCH_OPTIMIZERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>
#include <ha3/search/Optimizer.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(search);

struct OptimizerInitParam {
    OptimizerInitParam()
        : _reader(NULL)
    {}
    OptimizerInitParam(config::ResourceReader* reader, const KeyValueMap& kvMap)
        : _reader(reader)
        , _kvMap(kvMap)
    {}

    config::ResourceReader *_reader;
    KeyValueMap _kvMap;
};

static const std::string MODULE_FUNC_OPTIMIZER = "_Optimizer";
class OptimizerModuleFactory : public util::ModuleFactory
{
public:
    OptimizerModuleFactory() {}
    virtual ~OptimizerModuleFactory() {}
private:
    OptimizerModuleFactory(const OptimizerModuleFactory &);
    OptimizerModuleFactory& operator=(const OptimizerModuleFactory &);
public:
    virtual Optimizer* createOptimizer(const std::string &optimizerName, const OptimizerInitParam &param) = 0;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(OptimizerModuleFactory);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZERMODULEFACTORY_H
