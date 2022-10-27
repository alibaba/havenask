#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <suez/turing/common/LocalSearchService.h>
BEGIN_HA3_NAMESPACE(sql);

class TvfLocalSearchResource: public TvfResourceBase {
public:
    TvfLocalSearchResource(suez::turing::LocalSearchServicePtr localSearchService)
        : _localSearchService(localSearchService) {}
public:
    virtual std::string name() const {
        return "TvfLocalSearchResource";
    }
public:
    suez::turing::LocalSearchServicePtr getLocalSearchService() {
        return _localSearchService;
    }    
private:
    suez::turing::LocalSearchServicePtr _localSearchService;
};

HA3_TYPEDEF_PTR(TvfLocalSearchResource);
END_HA3_NAMESPACE(sql);
