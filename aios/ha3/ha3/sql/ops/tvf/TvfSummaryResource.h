#pragma once
#include <ha3/summary/SummaryProfileManager.h>

BEGIN_HA3_NAMESPACE(sql);

class TvfSummaryResource: public TvfResourceBase {
public:
    TvfSummaryResource(HA3_NS(summary)::SummaryProfileManagerPtr &summaryProfileMgrPtr)
        : _summaryProfileMgrPtr(summaryProfileMgrPtr) {}
public:
    virtual std::string name() const {
        return "TvfSummaryResource";
    }
public:
    HA3_NS(summary)::SummaryProfileManagerPtr getSummaryProfileManager() {
        return _summaryProfileMgrPtr;
    }
private:
    HA3_NS(summary)::SummaryProfileManagerPtr _summaryProfileMgrPtr;
};

HA3_TYPEDEF_PTR(TvfSummaryResource);
END_HA3_NAMESPACE(sql);
