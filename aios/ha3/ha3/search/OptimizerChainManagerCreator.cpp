#include <ha3/search/OptimizerChainManagerCreator.h>
#include <ha3/search/OptimizerModuleFactory.h>
#include <ha3/search/AuxiliaryChainOptimizer.h>
#include <build_service/plugin/PlugInManager.h>
#include <build_service/plugin/Module.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace build_service::plugin;
using namespace autil::legacy;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, OptimizerChainManagerCreator);

OptimizerChainManagerCreator::OptimizerChainManagerCreator(
        const ResourceReaderPtr &resourceReaderPtr)
    : _resourceReaderPtr(resourceReaderPtr)
{
}

OptimizerChainManagerCreator::~OptimizerChainManagerCreator() {
}

OptimizerChainManagerPtr OptimizerChainManagerCreator::createFromString(const std::string &configStr) {
    SearchOptimizerConfig optimizerConfig;
    if(!configStr.empty()) {
        try {
            FromJsonString(optimizerConfig, configStr);
        } catch (const ExceptionBase &e) {
            HA3_LOG(ERROR, "Catch exception in OptimizerChainManagerCreator: [%s]\n"
                    " FromJsonString fail: [%s]", e.what(), configStr.c_str());
            return OptimizerChainManagerPtr();
        }
    }
    return create(optimizerConfig);
}

OptimizerChainManagerPtr OptimizerChainManagerCreator::create(
        const SearchOptimizerConfig &optimizerConfig)
{
    OptimizerChainManagerPtr optimizerChainManagerPtr(new OptimizerChainManager);
    addBuildInOptimizers(optimizerChainManagerPtr);
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(_resourceReaderPtr, MODULE_FUNC_OPTIMIZER));
    optimizerChainManagerPtr->setPlugInManager(plugInManagerPtr);

    if(!plugInManagerPtr->addModules(optimizerConfig.getModuleInfos())) {
        HA3_LOG(ERROR, "Load optimizer module failed : %s",
                ToJsonString(optimizerConfig.getModuleInfos()).c_str());
        return OptimizerChainManagerPtr();
    }
    const OptimizerConfigInfos &optimizerConfigInfos =
        optimizerConfig.getOptimizerConfigInfos();
    for (size_t i = 0; i != optimizerConfigInfos.size(); i ++) {
        const OptimizerConfigInfo &optimizerConfigInfo = optimizerConfigInfos[i];
        Module *module = plugInManagerPtr->getModule(optimizerConfigInfo.moduleName);
        if(module == NULL) {
            HA3_LOG(ERROR, "Get module[%s] failed.", optimizerConfigInfo.moduleName.c_str());
            return OptimizerChainManagerPtr();
        }
        OptimizerModuleFactory *factory = dynamic_cast<OptimizerModuleFactory *>(module->getModuleFactory());
        if (factory == NULL) {
            HA3_LOG(ERROR, "Get module factory failed.");
            return OptimizerChainManagerPtr();
        }

        OptimizerInitParam param(_resourceReaderPtr.get(), optimizerConfigInfo.parameters);
        OptimizerPtr optimizerPtr(factory->createOptimizer(optimizerConfigInfo.optimizerName, param));
        if (!optimizerPtr) {
            HA3_LOG(ERROR, "create optimizer[%s] failed.", optimizerConfigInfo.optimizerName.c_str());
            return OptimizerChainManagerPtr();
        }
        if (!optimizerChainManagerPtr->addOptimizer(optimizerPtr)) {
            HA3_LOG(ERROR, "duplicate optimizer name [%s].", optimizerConfigInfo.optimizerName.c_str());
            return OptimizerChainManagerPtr();
        }
    }
    return optimizerChainManagerPtr;
}

void OptimizerChainManagerCreator::addBuildInOptimizers(
        OptimizerChainManagerPtr &optimizerChainManagerPtr)
{
    OptimizerPtr optimizerPtr(new AuxiliaryChainOptimizer());
    optimizerChainManagerPtr->addOptimizer(optimizerPtr);
}

END_HA3_NAMESPACE(search);

