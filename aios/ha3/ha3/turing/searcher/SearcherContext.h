#ifndef ISEARCH_SEARCHERCONTEXT_H
#define ISEARCH_SEARCHERCONTEXT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <suez/turing/search/GraphSearchContext.h>
#include <suez/turing/search/Biz.h>

BEGIN_HA3_NAMESPACE(turing);

class SearcherContext : public suez::turing::GraphSearchContext
{
public:
    SearcherContext(const suez::turing::SearchContextArgs &args,
                    const suez::turing::GraphRequest *request,
                    suez::turing::GraphResponse *response);

    ~SearcherContext();
private:
    SearcherContext(const SearcherContext &);
    SearcherContext& operator=(const SearcherContext &);
protected:
    std::string getBizMetricName() override;
    std::string getRequestSrc() override;
    void prepareQueryMetricsReporter(const suez::turing::BizPtr &biz) override;
    suez::turing::BasicMetricsCollectorPtr createMetricsCollector() override;
    void addEagleInfo() override;
    void fillMetrics() override;
    void reportMetrics() override;
    void formatErrorResult() override;
    void doFormatResult() override;
private:
    void fillSearchInfo(HA3_NS(monitor)::SessionMetricsCollector *collector,
                        common::ResultPtr &result);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherContext);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERCONTEXT_H
