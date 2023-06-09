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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/common/GlobalIdentifier.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class FetchSummaryClause : public ClauseBase {
public:
    FetchSummaryClause();
    ~FetchSummaryClause();
private:
    FetchSummaryClause(const FetchSummaryClause &);
    FetchSummaryClause& operator=(const FetchSummaryClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override {
        return;
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        return;
    }
    std::string toString() const override {
        return _originalString;
    }
public:
    void addGid(const std::string &clusterName,
                const GlobalIdentifier &gid)
    {
        _clusterNames.push_back(cluster2ZoneName(clusterName));
        _gids.push_back(gid);
    }
    void addRawPk(const std::string &clusterName,
                  const std::string &rawPk)
    {
        _clusterNames.push_back(cluster2ZoneName(clusterName));
        _rawPks.push_back(rawPk);
    }
    const std::vector<std::string> &getClusterNames() const {
        return _clusterNames;
    }
    const std::vector<GlobalIdentifier> &getGids() const {
        return _gids;
    }
    const std::vector<std::string> &getRawPks() const  {
        return _rawPks;
    }
    size_t getGidCount() const {
        return _gids.size();
    }
    size_t getRawPkCount() const {
        return _rawPks.size();
    }
    size_t getClusterCount() const {
        return _clusterNames.size();
    }
private:
    std::vector<std::string> _clusterNames;
    std::vector<GlobalIdentifier> _gids;
    std::vector<std::string> _rawPks;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FetchSummaryClause> FetchSummaryClausePtr;

} // namespace common
} // namespace isearch

