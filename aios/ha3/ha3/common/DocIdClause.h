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
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "indexlib/indexlib.h"

#include "ha3/common/ClauseBase.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace proto {
class Range;
}  // namespace proto
}  // namespace isearch

namespace isearch {
namespace common {

typedef std::vector<GlobalIdentifier> GlobalIdVector;
typedef std::vector<docid_t> DocIdVector;
typedef std::map<versionid_t, GlobalIdVector> GlobalIdVectorMap;
typedef std::pair<hashid_t, FullIndexVersion> HashidVersion;
typedef std::set<HashidVersion> HashidVersionSet;

class DocIdClause : public ClauseBase
{
public:
    DocIdClause()
        : _summaryProfileName(DEFAULT_SUMMARY_PROFILE)
        , _signature(0)
    {
    }
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override {
        return _originalString;
    }
public:
    void addGlobalId(const GlobalIdentifier &globalId);
    void getGlobalIdVector(const proto::Range &hashIdRange,
                           FullIndexVersion fullVersion,
                           GlobalIdVector &gids);
    void getGlobalIdVector(const proto::Range &hashIdRange,
                           GlobalIdVector &gids);
    void getGlobalIdVectorMap(const proto::Range &hashIdRange,
                              FullIndexVersion fullVersion,
                              GlobalIdVectorMap &globalIdVectorMap);
    void getHashidVersionSet(HashidVersionSet &hashidVersionSet);
    void getHashidVector(std::vector<hashid_t> &hashids);

    void setQueryString(const std::string &queryString) {
        _queryString = queryString;
    }
    const std::string& getQueryString() const {
        return _queryString;
    };
    void setTermVector(const TermVector &termVector) {
        _termVector = termVector;
    }
    const TermVector& getTermVector() const {
        return _termVector;
    }
    std::string getSummaryProfileName() const {
        return _summaryProfileName;
    }
    void setSummaryProfileName(const std::string &summaryProfileName) {
        _summaryProfileName = summaryProfileName;
    }
    const GlobalIdVector& getGlobalIdVector() const {
        return _globalIdVector;
    }
    void setGlobalIdVector(const GlobalIdVector &globalIdVector) {
        _globalIdVector = globalIdVector;
    }

    int64_t getSignature() const { return _signature; }
    void setSignature(int64_t signature) { _signature = signature; }

private:
    GlobalIdVector _globalIdVector;
    TermVector _termVector;
    std::string _queryString;
    std::string _summaryProfileName;
    int64_t _signature;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::map<std::string, DocIdClause*> DocIdClauseMap;

} // namespace common
} // namespace isearch
