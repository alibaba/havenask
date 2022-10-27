#ifndef ISEARCH_BASICSEARCHERCONTEXT_H
#define ISEARCH_BASICSEARCHERCONTEXT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <suez/turing/search/GraphSearchContext.h>
#include <suez/turing/search/Biz.h>

BEGIN_HA3_NAMESPACE(turing);

class BasicSearcherContext : public suez::turing::GraphSearchContext
{
public:
    BasicSearcherContext(const suez::turing::SearchContextArgs &args,
                         const suez::turing::GraphRequest *request,
                         suez::turing::GraphResponse *response);

    ~BasicSearcherContext();
private:
    BasicSearcherContext(const BasicSearcherContext &);
    BasicSearcherContext& operator=(const BasicSearcherContext &);
protected:
    void formatErrorResult() override;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BasicSearcherContext);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERCONTEXT_H
