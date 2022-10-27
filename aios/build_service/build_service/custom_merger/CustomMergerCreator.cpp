#include "build_service/custom_merger/CustomMergerCreator.h"
#include "build_service/util/Monitor.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/custom_merger/BuildinCustomMergerFactory.h"

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergerCreator);

CustomMergerCreator::CustomMergerCreator() 
    : _metricProvider(NULL)
{
}

CustomMergerCreator::~CustomMergerCreator() {
}

CustomMergerPtr CustomMergerCreator::create(
        const config::MergePluginConfig &mergeConfig,
        MergeResourceProviderPtr& resourceProvider) 
{
    PlugInManagerPtr plugInManager(new PlugInManager(
                    _resourceReaderPtr, _resourceReaderPtr->getPluginPath(), MODULE_FUNC_MERGER));
    if (!plugInManager->addModule(mergeConfig.getModuleInfo())) {
        return CustomMergerPtr();
    }

    BuildinCustomMergerFactory buildInFactory;
    CustomMergerFactory* factory = NULL;  
    const ProcessorInfo& processorInfo = mergeConfig.getProcessorInfo();

    factory = &buildInFactory;
    string moduleName = processorInfo.moduleName;
    if (PlugInManager::isBuildInModule(moduleName)) {
        factory = &buildInFactory;
    } else {
        Module* module = plugInManager->getModule(moduleName);
        if (!module) {
            string errorMsg = "Init custom merger failed. no module name["
                              + moduleName + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return CustomMergerPtr();
        }
        factory = dynamic_cast<CustomMergerFactory*>(module->getModuleFactory());
        if (!factory) {
            string errorMsg = "Invalid module[" + moduleName + "].";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return CustomMergerPtr();
        }
    }
    CustomMerger::CustomMergerInitParam param; 
    param.resourceProvider = resourceProvider;
    param.parameters = processorInfo.parameters;
    param.metricProvider = _metricProvider;
    param.resourceReader = _resourceReaderPtr.get();
    
    CustomMergerPtr customMerger(factory->createCustomMerger(processorInfo.className));
    if (!customMerger) {
        string errorMsg = "create custom merger[" + processorInfo.className + "] from factory failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return CustomMergerPtr();
    }

    if (!customMerger->init(param))
    {
        string errorMsg = "Init custom merger [" + processorInfo.className + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return CustomMergerPtr();
    }
    return customMerger;
}

CustomMerger::CustomMergerInitParam CustomMergerCreator::getTestParam()
{
    CustomMerger::CustomMergerInitParam param; // add index provider
    KeyValueMap parameters;
    //param.resourceProvider = resourceProvider;
    param.parameters = parameters;
    return param;
}

}
}
