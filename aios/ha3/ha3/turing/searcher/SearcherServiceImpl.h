#pragma once

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/GraphServiceImpl.h>
#include <suez/turing/proto/Search.pb.h>

BEGIN_HA3_NAMESPACE(turing);

class SearcherServiceImpl: public suez::turing::GraphServiceImpl
{
public:
    SearcherServiceImpl();
    ~SearcherServiceImpl();
private:
    SearcherServiceImpl(const SearcherServiceImpl &);
    SearcherServiceImpl& operator=(const SearcherServiceImpl &);
protected:
    bool doInit() override;
    suez::turing::ServiceSnapshotPtr doCreateServiceSnapshot() override;
    suez::turing::SearchContext *doCreateContext(
            const suez::turing::SearchContextArgs &args,
            const suez::turing::GraphRequest *request,
            suez::turing::GraphResponse *response) override;
    bool doInitRpcServer(suez::RpcServer *rpcServer) override;
private:
    std::set<std::string> _basicTuringBizNames;
    std::string _defaultAggStr;
    std::string _paraWaysStr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherServiceImpl);

END_HA3_NAMESPACE(turing);
