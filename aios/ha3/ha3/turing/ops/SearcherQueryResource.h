#ifndef ISEARCH_SEARCHERQUERYRESOURCE_H
#define ISEARCH_SEARCHERQUERYRESOURCE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

#include <suez/turing/common/QueryResource.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/SearcherCacheInfo.h>

BEGIN_HA3_NAMESPACE(turing);

class SearcherQueryResource : public tensorflow::QueryResource
{
public:
    SearcherQueryResource() {}
    ~SearcherQueryResource() {}
private:
    SearcherQueryResource(const SearcherQueryResource &);
    SearcherQueryResource& operator=(const SearcherQueryResource &);
public:
    suez::turing::TimeoutTerminator *getSeekTimeoutTerminator() const { return _seekTimeoutTerminator.get(); }
    void setSeekTimeoutTerminator(const suez::turing::TimeoutTerminatorPtr &timeoutTerminator) {
        _seekTimeoutTerminator = timeoutTerminator;
    }

public:
    HA3_NS(monitor)::SessionMetricsCollector *sessionMetricsCollector = nullptr;
    std::vector<search::IndexPartitionReaderWrapperPtr> paraSeekReaderWrapperVec;
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    search::SearchCommonResourcePtr commonResource;
    search::SearchPartitionResourcePtr partitionResource;
    search::SearchRuntimeResourcePtr runtimeResource;
    search::SearcherCacheInfoPtr searcherCacheInfo;
private:
    common::TimeoutTerminatorPtr _seekTimeoutTerminator;
private:
    HA3_LOG_DECLARE();
};
typedef std::shared_ptr<SearcherQueryResource> SearcherQueryResourcePtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERQUERYRESOURCE_H
