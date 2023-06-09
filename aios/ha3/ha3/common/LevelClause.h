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
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

enum LevelQueryType {
    ONLY_FIRST_LEVEL,
    CHECK_FIRST_LEVEL,
    BOTH_LEVEL,
};

typedef std::vector<std::vector<std::string> > ClusterLists;

class LevelClause : public ClauseBase
{
public:
    LevelClause()
        : _minDocs(0)
        , _levelQueryType(CHECK_FIRST_LEVEL)
        , _useLevel(true)
    {}
    
    ~LevelClause() {}
private:
    LevelClause(const LevelClause &);
    LevelClause& operator=(const LevelClause &);
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
    bool useLevel() const {
        return _useLevel;
    }
    void setUseLevel(bool f) {
        _useLevel = f;
    }
    LevelQueryType getLevelQueryType() const {
        return _levelQueryType;
    }
    void setLevelQueryType(LevelQueryType levelQueryType) {
        _levelQueryType = levelQueryType;
    }
    const ClusterLists& getLevelClusters() const {
        return _levelClusters;
    }
    void setLevelClusters(const ClusterLists& levelClusters) {
        _levelClusters = levelClusters;
        for (size_t i = 0; i < _levelClusters.size(); ++i) {
            for (size_t j = 0; j < _levelClusters[i].size(); ++j) {
                _levelClusters[i][j] = cluster2ZoneName(_levelClusters[i][j]);
            }
        }
    }
    const std::string& getSecondLevelCluster() const {
        if (_levelClusters.empty() || _levelClusters[0].empty()) {
            return HA3_EMPTY_STRING;
        }
        return _levelClusters[0][0];
    }
    void setSecondLevelCluster(const std::string& cluster) {
        _levelClusters.clear();
        _levelClusters.push_back(std::vector<std::string>(1, cluster2ZoneName(cluster)));
    }
    uint32_t getMinDocs() const {
        return _minDocs;
    }
    void setMinDocs(uint32_t minDocs) {
        _minDocs = minDocs;
    }
    bool maySearchSecondLevel() const {
        return _useLevel
            && (_levelQueryType == CHECK_FIRST_LEVEL
                || _levelQueryType == BOTH_LEVEL);
    }
private:
    ClusterLists _levelClusters;

    uint32_t _minDocs;
    LevelQueryType _levelQueryType;
    bool _useLevel;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LevelClause> LevelClausePtr;

} // namespace common
} // namespace isearch

