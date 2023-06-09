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

#include <stdint.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class HealthCheckClause : public ClauseBase
{
public:
    HealthCheckClause();
    ~HealthCheckClause();
private:
    HealthCheckClause(const HealthCheckClause &);
    HealthCheckClause& operator=(const HealthCheckClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    bool isCheck() const {return _check;}
    void setCheck(bool check) {_check = check;}
    int32_t getCheckTimes() const {return _checkTimes;}
    void setCheckTimes(int32_t checkTimes) {_checkTimes = checkTimes;}
    void setRecoverTime(int64_t t) {_recoverTime = t;}
    int64_t getRecoverTime() const {return _recoverTime;}
private:
    bool _check;
    int32_t _checkTimes;
    int64_t _recoverTime;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<HealthCheckClause> HealthCheckClausePtr;

} // namespace common
} // namespace isearch

