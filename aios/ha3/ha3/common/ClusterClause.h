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
#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

typedef std::map<std::string, autil::RangeVec> ClusterPartIdPairsMap;
typedef std::map<std::string, std::vector<hashid_t> > ClusterPartIdsMap;

class ClusterClause : public ClauseBase
{
public:
    ClusterClause();
    ~ClusterClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override {
        return _originalString;
    }
public:
    const ClusterPartIdsMap& getClusterPartIdsMap() const;
    const ClusterPartIdPairsMap& getClusterPartIdPairsMap() const;
    bool emptyPartIds() const;

    /**deprecated**/
    void addClusterPartIds(const std::string &clusterName,
                           const std::vector<hashid_t> &partids);

    void addClusterPartIds(const std::string &clusterName,
                           const std::vector<std::pair<hashid_t, hashid_t> > &partids);

    bool getClusterPartIds(const std::string &clusterName,
                           std::vector<std::pair<hashid_t, hashid_t> > &partids) const;

    void clearClusterName();
    size_t getClusterNameCount() const;
    void addClusterName(const std::string &clusterName);
    std::string getClusterName(size_t index = 0) const;
    const std::vector<std::string> &getClusterNameVector() const;
    void setTotalSearchPartCnt(uint32_t partCnt);
    uint32_t getToalSearchPartCnt() const;
private:
    void serializeExtFields(autil::DataBuffer &dataBuffer) const;
    void deserializeExtFields(autil::DataBuffer &dataBuffer);
private:
    ClusterPartIdsMap _clusterPartIdsMap;
    ClusterPartIdPairsMap _clusterPartIdPairsMap;
    std::vector<std::string> _clusterNameVec;
    uint32_t _totalSearchPartCnt;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ClusterClause> ClusterClausePtr;

} // namespace common
} // namespace isearch
