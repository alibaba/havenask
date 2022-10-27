#ifndef ISEARCH_ANALYZERFACTORYINIT_H
#define ISEARCH_ANALYZERFACTORYINIT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/analyzer/AnalyzerFactory.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AnalyzerFactoryInit
{
public:
    static void initFactory(build_service::analyzer::AnalyzerFactory &factory);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ANALYZERFACTORYINIT_H
