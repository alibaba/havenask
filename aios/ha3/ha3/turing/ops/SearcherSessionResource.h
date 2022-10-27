#ifndef ISEARCH_SEARCHERSESSIONRESOURCE_H
#define ISEARCH_SEARCHERSESSIONRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/config/ConfigAdapter.h>
#include <ha3/util/NetFunction.h>

BEGIN_HA3_NAMESPACE(turing);

class SearcherSessionResource: public tensorflow::SessionResource
{
public:
    SearcherSessionResource(uint32_t count);
    ~SearcherSessionResource();
private:
    SearcherSessionResource(const SearcherSessionResource &);
    SearcherSessionResource& operator=(const SearcherSessionResource &);
public:
    HA3_NS(service)::SearcherResourcePtr searcherResource;
    HA3_NS(config)::ConfigAdapterPtr configAdapter;
    std::string ip;
public:
    uint32_t getIp() const {
        return util::NetFunction::encodeIp(ip);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherSessionResource);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERSESSIONRESOURCE_H
