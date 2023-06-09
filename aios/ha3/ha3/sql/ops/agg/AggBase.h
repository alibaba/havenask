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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/ops/agg/AggFuncDesc.h"
#include "table/Table.h"

namespace isearch {
namespace sql {
class AggFuncManager;
class Aggregator;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class AggBase {
public:
    AggBase() {}
    virtual ~AggBase() {}

private:
    AggBase(const AggBase &);
    AggBase &operator=(const AggBase &);

public:
    bool init(const AggFuncManager *aggFuncManager,
              const std::vector<std::string> &groupKeyVec,
              const std::vector<std::string> &outputFields,
              const std::vector<AggFuncDesc> &aggFuncDesc);
    virtual bool compute(table::TablePtr &input) = 0;
    virtual bool finalize() {
        return true;
    }
    virtual table::TablePtr getTable() = 0;
    virtual void getStatistics(uint64_t &collectTime,
                               uint64_t &outputAccTime,
                               uint64_t &mergeTime,
                               uint64_t &outputResultTime,
                               uint64_t &aggPoolSize) const = 0;
protected:
    bool computeAggregator(table::TablePtr &input,
                           const std::vector<AggFuncDesc> &aggFuncDesc,
                           Aggregator &aggregator,
                           bool &aggregatorReady);

private:
    virtual bool doInit() {
        return true;
    }

protected:
    const AggFuncManager *_aggFuncManager = nullptr;
    std::vector<std::string> _groupKeyVec;
    std::vector<std::string> _outputFields;
    std::vector<AggFuncDesc> _aggFuncDesc;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggBase> AggBasePtr;
} // namespace sql
} // namespace isearch
