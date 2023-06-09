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
#include <map>
#include <string>

#include "ha3/common/ClauseBase.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
struct RankHint;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

typedef std::map<std::string, int32_t> SingleFieldBoostConfig;
typedef std::map<std::string, SingleFieldBoostConfig> FieldBoostDescription;

class RankClause : public ClauseBase
{
public:
    RankClause();
    ~RankClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRankProfileName(const std::string &rankProfileName);
    std::string getRankProfileName() const;

    void setFieldBoostDescription(const FieldBoostDescription& des);
    const FieldBoostDescription& getFieldBoostDescription() const;

    void setRankHint(RankHint* rankHint);
    const RankHint* getRankHint() const;
private:
    std::string _rankProfileName;
    FieldBoostDescription _fieldBoostDescription;
    RankHint *_rankHint;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

