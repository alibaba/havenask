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

#include <stddef.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class Request;
}  // namespace common
namespace monitor {
class SessionMetricsCollector;
}  // namespace monitor
namespace search {
class IndexPartitionReaderWrapper;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace turing {
class Ha3BizMeta;
}  // namespace turing

namespace search {

struct OptimizeInitParam {
    const std::string &optimizeOption;
    const common::Request *request;

    OptimizeInitParam(const std::string &option,
                      const common::Request *request)
        : optimizeOption(option)
        , request(request)
    {
    }
};

struct OptimizeParam {
    const common::Request *request;
    IndexPartitionReaderWrapper *readerWrapper;
    monitor::SessionMetricsCollector *sessionMetricsCollector;
    const isearch::turing::Ha3BizMeta *ha3BizMeta;

    OptimizeParam() {
        request = NULL;
        readerWrapper = NULL;
        sessionMetricsCollector = NULL;
        ha3BizMeta = NULL;
    }
};

class Optimizer;

typedef std::shared_ptr<Optimizer> OptimizerPtr;

class Optimizer
{
public:
    Optimizer() {}
    virtual ~Optimizer() {}
public:
    virtual bool init(OptimizeInitParam *param) {
        return true;
    }
    virtual OptimizerPtr clone() = 0;
    virtual std::string getName() const = 0;
    virtual bool optimize(OptimizeParam *param) = 0;
    virtual void disableTruncate() = 0;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Optimizer> OptimizerPtr;

} // namespace search
} // namespace isearch
