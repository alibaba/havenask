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

#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BasicSearcherContext> BasicSearcherContextPtr;

} // namespace turing
} // namespace isearch

