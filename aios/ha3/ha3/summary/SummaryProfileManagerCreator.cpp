#include <ha3/summary/SummaryProfileManagerCreator.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/config/SummaryProfileConfig.h>
#include <ha3/summary/SummaryModuleFactory.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryProfileManagerCreator);

SummaryProfileManagerPtr SummaryProfileManagerCreator::createFromString(
        const std::string &configStr)
{
    SummaryProfileConfig summaryProfileConfig;
    if (!configStr.empty()) {
        try {
            FromJsonString(summaryProfileConfig, configStr);
        } catch (const ExceptionBase &e) {
            HA3_LOG(ERROR, "Create SummaryProfileManager Failed. ConfigString[%s], Exception[%s]",
                    configStr.c_str(), e.what());
            return SummaryProfileManagerPtr();
        }
    }
    return create(summaryProfileConfig);
}


SummaryProfileManagerPtr SummaryProfileManagerCreator::create(
        const SummaryProfileConfig &summaryProfileConfig)
{
    SummaryProfileInfos summaryProfileInfos =
        summaryProfileConfig.getSummaryProfileInfos();
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(
                    _resourceReaderPtr, MODULE_FUNC_SUMMARY));
    if (!plugInManagerPtr->addModules(summaryProfileConfig.getModuleInfos())) {
        HA3_LOG(ERROR, "load to create summary modules : %s",
                ToJsonString(summaryProfileConfig.getModuleInfos()).c_str());
        return SummaryProfileManagerPtr();
    }
    SummaryProfileManagerPtr summaryProfileManagerPtr(
            new SummaryProfileManager(plugInManagerPtr));

    for (size_t i = 0; i < summaryProfileInfos.size(); i++) {
        summaryProfileManagerPtr->addSummaryProfile(
                new SummaryProfile(summaryProfileInfos[i],
                        _hitSummarySchema.get()));
    }
    if (!summaryProfileManagerPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, _metricsReporter)) {
        HA3_LOG(ERROR, "Init summaryProfileManager fail!");
        return SummaryProfileManagerPtr();
    }
    summaryProfileManagerPtr->setRequiredAttributeFields(
            summaryProfileConfig.getRequiredAttributeFields());
    return summaryProfileManagerPtr;
}

END_HA3_NAMESPACE(summary);
