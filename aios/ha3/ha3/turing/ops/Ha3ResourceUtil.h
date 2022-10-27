#ifndef ISEARCH_RESOURCEUTIL_H
#define ISEARCH_RESOURCEUTIL_H
#include <suez/turing/common/QueryResource.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/Request.h>
#include <ha3/service/SearcherResource.h>
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

BEGIN_HA3_NAMESPACE(turing);

class Ha3ResourceUtil {
public:
    static HA3_NS(search)::SearchCommonResourcePtr createSearchCommonResource(
            const HA3_NS(common)::Request *request,
            autil::mem_pool::Pool *pool,
            HA3_NS(monitor)::SessionMetricsCollector *collectorPtr,
            HA3_NS(common)::TimeoutTerminator *timeoutTerminator,
            HA3_NS(common)::Tracer *tracer,
            HA3_NS(service)::SearcherResourcePtr &resourcePtr,
            suez::turing::SuezCavaAllocator *cavaAllocator,
            const std::map<size_t, ::cava::CavaJitModulePtr> &cavaJitModules);

    static HA3_NS(search)::SearchPartitionResourcePtr createSearchPartitionResource(
            const HA3_NS(common)::Request *request,
            const HA3_NS(search)::IndexPartitionReaderWrapperPtr &indexPartReaderWrapperPtr,
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &partitionReaderSnapshot,
            HA3_NS(search)::SearchCommonResourcePtr &commonResource);

    static HA3_NS(search)::SearchRuntimeResourcePtr createSearchRuntimeResource(
            const HA3_NS(common)::Request *request,
            const HA3_NS(service)::SearcherResourcePtr &searcherResource,
            const HA3_NS(search)::SearchCommonResourcePtr &commonResource,
            suez::turing::AttributeExpressionCreator *attributeExpressionCreator);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_RESOURCEUTIL_H
