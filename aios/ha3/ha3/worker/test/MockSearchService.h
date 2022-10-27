#ifndef ISEARCH_MOCKSEARCHSERVICE_H
#define ISEARCH_MOCKSEARCHSERVICE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <multi_call/interface/SearchService.h>

BEGIN_HA3_NAMESPACE(worker);

class MockSearchService : public multi_call::SearchService
{
public:
    MockSearchService() {}
    ~MockSearchService() {}
public:
    MOCK_METHOD2(init, bool(const multi_call::MultiCallConfig &mcConfig,
                            kmonitor::KMonitor *kMonitor));
    MOCK_METHOD2(updateFlowConfig, bool(const std::string &zoneBizName,
                    const autil::legacy::json::JsonMap *flowConf));
    MOCK_METHOD2(search, void(const multi_call::SearchParam &param,
                              multi_call::ReplyPtr &repl));
    MOCK_CONST_METHOD0(getBizNames, std::vector<std::string>());
};

class MockSearchService2 : public multi_call::SearchService
{
public:
    MockSearchService2() {}
    ~MockSearchService2() {}
public:
    std::vector<std::string> getBizNames() const override{
        return _bizNames;
    }
public:
    std::vector<std::string> _bizNames;
};

HA3_TYPEDEF_PTR(MockSearchService);
HA3_TYPEDEF_PTR(MockSearchService2);

END_HA3_NAMESPACE(worker);

#endif //ISEARCH_MOCKSEARCHSERVICE_H
