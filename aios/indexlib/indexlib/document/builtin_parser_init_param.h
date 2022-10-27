#ifndef __INDEXLIB_BUILTIN_PARSER_INIT_RESOURCE_H
#define __INDEXLIB_BUILTIN_PARSER_INIT_RESOURCE_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/document_init_param.h"

DECLARE_REFERENCE_CLASS(document, DocumentRewriter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
        
IE_NAMESPACE_BEGIN(document);

struct BuiltInParserInitResource
{
    util::CounterMapPtr counterMap;
    misc::MetricProviderPtr metricProvider = nullptr;
    std::vector<DocumentRewriterPtr> docRewriters;
};

typedef DocumentInitParamTyped<BuiltInParserInitResource> BuiltInParserInitParam;
DEFINE_SHARED_PTR(BuiltInParserInitParam);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_BUILTIN_PARSER_INIT_RESOURCE_H
