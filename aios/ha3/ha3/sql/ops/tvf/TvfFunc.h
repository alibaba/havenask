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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/turing/common/ModelConfig.h"
#include "kmonitor/client/MetricsReporter.h"
#include "table/Table.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace common {
class QueryInfo;
} // namespace common
namespace sql {
class TvfResourceContainer;
} // namespace sql
} // namespace isearch
namespace navi {
class GraphMemoryPoolResource;
} // namespace navi

namespace suez {
namespace turing {
class SuezCavaAllocator;
}
} // namespace suez

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace isearch {
namespace sql {

struct TvfFuncInitContext {
    std::vector<std::string> params;
    autil::mem_pool::Pool *queryPool = nullptr;
    navi::GraphMemoryPoolResource *poolResource = nullptr;
    TvfResourceContainer *tvfResourceContainer = nullptr;
    suez::turing::SuezCavaAllocator *suezCavaAllocator = nullptr;
    const common::QueryInfo *queryInfo = nullptr;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    isearch::turing::ModelConfigMap *modelConfigMap;
    multi_call::QuerySession *querySession;
    kmonitor::MetricsReporter *metricReporter = nullptr;
};

class TvfFunc {
public:
    TvfFunc() {}
    virtual ~TvfFunc() {}

private:
    TvfFunc(const TvfFunc &);
    TvfFunc &operator=(const TvfFunc &);

public:
    virtual bool init(TvfFuncInitContext &context) = 0;
    virtual bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) = 0;

public:
    std::string getName() const {
        return _name;
    }
    void setName(const std::string &name) {
        _name = name;
    }
    KernelScope getScope() const {
        return _scope;
    }
    void setScope(KernelScope scope) {
        _scope = scope;
    }

protected:
    AUTIL_LOG_DECLARE();

private:
    std::string _name;
    KernelScope _scope;
};
typedef std::shared_ptr<TvfFunc> TvfFuncPtr;
class OnePassTvfFunc : public TvfFunc {
public:
    OnePassTvfFunc() {}
    virtual ~OnePassTvfFunc() {}

public:
    bool compute(const table::TablePtr &input, bool eof, table::TablePtr &output) override;

protected:
    virtual bool doCompute(const table::TablePtr &input, table::TablePtr &output) = 0;

protected:
    table::TablePtr _table;
};

typedef std::shared_ptr<OnePassTvfFunc> OnePassTvfFuncPtr;
} // namespace sql
} // namespace isearch
