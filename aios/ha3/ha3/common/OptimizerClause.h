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

#include <assert.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class OptimizerClause : public ClauseBase
{
public:
    OptimizerClause();
    ~OptimizerClause();
private:
    OptimizerClause(const OptimizerClause &);
    OptimizerClause& operator=(const OptimizerClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    uint32_t getOptimizeCount() const {
        return _optimizeNames.size();
    }
    uint32_t getOptimizeOptionCount() const {
        return _optimizeOptions.size();
    }
    const std::string &getOptimizeName(uint32_t pos) const {
        assert(pos < _optimizeNames.size());
        return _optimizeNames[pos];
    }
    void addOptimizerName(const std::string &name) {
        _optimizeNames.push_back(name);
    }

    const std::string &getOptimizeOption(uint32_t pos) const {
        assert(pos < _optimizeOptions.size());
        return _optimizeOptions[pos];
    }
    void addOptimizerOption(const std::string &option) {
        _optimizeOptions.push_back(option);
    }
private:
    std::vector<std::string> _optimizeNames;
    std::vector<std::string> _optimizeOptions;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OptimizerClause> OptimizerClausePtr;

} // namespace common
} // namespace isearch

