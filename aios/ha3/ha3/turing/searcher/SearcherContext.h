/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Result.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "suez/turing/metrics/BasicBizMetrics.h"
#include "suez/turing/search/GraphSearchContext.h"

namespace suez {
namespace turing {
class GraphRequest;
class GraphResponse;
struct SearchContextArgs;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {

class SearcherContext : public suez::turing::GraphSearchContext
{
public:
    SearcherContext(const suez::turing::SearchContextArgs &args,
                    const suez::turing::GraphRequest *request,
                    suez::turing::GraphResponse *response);

    ~SearcherContext();
    friend class SearcherContextTest;
private:
    SearcherContext(const SearcherContext &);
    SearcherContext& operator=(const SearcherContext &);
public:
    void init() override;
protected:
    void addExtraTags(std::map<std::string, std::string> &tagsMap) override;
    void fillQueryResource() override;
    void reportMetrics() override;
    void formatErrorResult() override;
    void doFormatResult() override;
private:
    suez::turing::BasicMetricsCollectorPtr createMetricsCollector();
    void addEagleInfo();
    void fillSearchInfo(isearch::monitor::SessionMetricsCollector *collector,
                        common::ResultPtr &result);
    void collectMetricAndSearchInfo();
private:
    isearch::monitor::SessionMetricsCollectorPtr _sessionMetricsCollector;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherContext> SearcherContextPtr;

} // namespace turing
} // namespace isearch
