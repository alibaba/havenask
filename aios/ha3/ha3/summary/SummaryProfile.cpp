#include <ha3/summary/SummaryProfile.h>
#include <ha3/summary/DefaultSummaryExtractor.h>
#include <build_service/plugin/Module.h>
#include <ha3/summary/SummaryModuleFactory.h>

using namespace std;
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryProfile);

SummaryProfile::SummaryProfile(const SummaryProfileInfo &summaryProfileInfo,
                               HitSummarySchema *hitSummarySchema)
    : _summaryProfileInfo(summaryProfileInfo)
    , _summaryExtractorChain(NULL)
    , _hitSummarySchema(hitSummarySchema)
{
    _summaryProfileInfo.convertConfigMapToVector(*hitSummarySchema);
}

SummaryProfile::~SummaryProfile() {
    if (_summaryExtractorChain) {
        _summaryExtractorChain->destroy();
        _summaryExtractorChain = NULL;
    }
}

bool SummaryProfile::init(build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
                          ResourceReaderPtr &resourceReaderPtr,
                          const CavaPluginManagerPtr &cavaPluginManagerPtr,
                          kmonitor::MetricsReporter *metricsReporter)
{
    if (_summaryExtractorChain) {
        return true;
    }
    const ExtractorInfoVector &extractorInfos = _summaryProfileInfo._extractorInfos;
    _summaryExtractorChain = new SummaryExtractorChain;
    bool ret = true;
    for (size_t i = 0; i < extractorInfos.size(); i++) {
        const ExtractorInfo &extractorInfo = extractorInfos[i];
        SummaryExtractor *extractor =
            createSummaryExtractor(plugInManagerPtr, resourceReaderPtr, extractorInfo,
                    cavaPluginManagerPtr, metricsReporter);
        if (extractor != NULL) {
            _summaryExtractorChain->addSummaryExtractor(extractor);
        } else {
            ret = false;
            break;
        }
    }
    return ret;
}

SummaryExtractor* SummaryProfile::createSummaryExtractor(
        build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
        config::ResourceReaderPtr &resourceReaderPtr,
        const ExtractorInfo &extractorInfo,
        const CavaPluginManagerPtr &cavaPluginManagerPtr,
        kmonitor::MetricsReporter *metricsReporter)
{
    SummaryExtractor *extractor = NULL;
    if (PlugInManager::isBuildInModule(extractorInfo._moduleName)){
        if(extractorInfo._extractorName == "DefaultSummaryExtractor") {
            extractor = new DefaultSummaryExtractor();
        } else if(extractorInfo._extractorName == "CavaSummaryExtractor") {
            extractor = new CavaSummaryExtractor(extractorInfo._parameters,
                    cavaPluginManagerPtr, metricsReporter);
        } else{
            HA3_LOG(ERROR, "failed to init SummaryProfile[%s] for BuildInModule",
                    extractorInfo._extractorName.c_str());
            return NULL;
        }
    } else {
        Module *module = plugInManagerPtr->getModule(extractorInfo._moduleName);
        if (module == NULL) {
            HA3_LOG(ERROR, "Get module [%s] failed1. module[%p]",
                    extractorInfo._moduleName.c_str(), module);
            return NULL;
        }
        SummaryModuleFactory *factory =
            dynamic_cast<SummaryModuleFactory *>(module->getModuleFactory());
        if (!factory) {
            HA3_LOG(ERROR, "Get SummaryModuleFactory failed!");
            return NULL;
        }
        extractor = factory->createSummaryExtractor(
                extractorInfo._extractorName.c_str(),
                extractorInfo._parameters, resourceReaderPtr.get(),
                _hitSummarySchema);
    }
    return extractor;
}

SummaryExtractorChain* SummaryProfile::createSummaryExtractorChain() const
{
    assert(_summaryExtractorChain);
    SummaryExtractorChain* summaryExtractorChain = _summaryExtractorChain->clone();
    return summaryExtractorChain;
}

string SummaryProfile::getProfileName() const {
    return _summaryProfileInfo._summaryProfileName;
}

END_HA3_NAMESPACE(summary);
