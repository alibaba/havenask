#ifndef ISEARCH_SUMMARYPROFILEMANAGER_H
#define ISEARCH_SUMMARYPROFILEMANAGER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/summary/SummaryProfile.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>

BEGIN_HA3_NAMESPACE(summary);

class SummaryProfileManager
{
public:
    typedef std::map<std::string, SummaryProfile*> SummaryProfiles;
public:
    SummaryProfileManager(build_service::plugin::PlugInManagerPtr plugInManagerPtr);
    ~SummaryProfileManager();
public:
    const SummaryProfile* getSummaryProfile(const std::string &summaryProfileName) const;
    void addSummaryProfile(SummaryProfile *summaryProfile);
    bool init(config::ResourceReaderPtr &resourceReaderPtr,
              const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
              kmonitor::MetricsReporter *metricsReporter);
    void setRequiredAttributeFields(const std::vector<std::string> &fieldVec);
    const std::vector<std::string>& getRequiredAttributeFields() const {
        return _requiredAttributeFields;
    }
private:
    void clear();
private:
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    SummaryProfiles _summaryProfiles;
    std::vector<std::string> _requiredAttributeFields;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryProfileManager);

END_HA3_NAMESPACE(summary);

#endif //ISEARCH_SUMMARYPROFILEMANAGER_H
