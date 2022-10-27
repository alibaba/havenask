#include <ha3/summary/SummaryProfileManager.h>

using namespace std;
using namespace build_service::plugin;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(summary);
HA3_LOG_SETUP(summary, SummaryProfileManager);

SummaryProfileManager::SummaryProfileManager(PlugInManagerPtr plugInManagerPtr) {
    _plugInManagerPtr = plugInManagerPtr;
}

SummaryProfileManager::~SummaryProfileManager() {
    clear();
}

const SummaryProfile* SummaryProfileManager::
getSummaryProfile(const std::string &summaryProfileName) const
{
    SummaryProfiles::const_iterator it = _summaryProfiles.find(summaryProfileName);
    if (it == _summaryProfiles.end()) {
        return NULL;
    } else {
        return it->second;
    }
}

void SummaryProfileManager::addSummaryProfile(SummaryProfile *summaryProfile) {
    string profileName = summaryProfile->getProfileName();
    SummaryProfiles::const_iterator it = _summaryProfiles.find(profileName);
    if (it != _summaryProfiles.end()) {
        delete it->second;
    }
    _summaryProfiles[profileName] = summaryProfile;
}

bool SummaryProfileManager::init(config::ResourceReaderPtr &resourceReaderPtr,
                                 const CavaPluginManagerPtr &cavaPluginManagerPtr,
                                 kmonitor::MetricsReporter *metricsReporter) {
    for (SummaryProfiles::iterator it = _summaryProfiles.begin();
         it != _summaryProfiles.end(); ++it)
    {
        if (!it->second->init(_plugInManagerPtr, resourceReaderPtr,
                              cavaPluginManagerPtr, metricsReporter))
        {
            return false;
        }
    }
    return true;
}

void SummaryProfileManager::setRequiredAttributeFields(
        const vector<string> &fieldVec)
{
    _requiredAttributeFields = fieldVec;
}

void SummaryProfileManager::clear() {
    for (SummaryProfiles::iterator it = _summaryProfiles.begin();
         it != _summaryProfiles.end(); ++it)
    {
        delete it->second;
    }
    _summaryProfiles.clear();
}

END_HA3_NAMESPACE(summary);
