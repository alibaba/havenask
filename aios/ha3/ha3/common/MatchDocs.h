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
#include <string>
#include <vector>

#include "indexlib/indexlib.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace common {

class MatchDocs
{
public:
    MatchDocs();
    ~MatchDocs();
public:
    uint32_t size() const;
    void resize(uint32_t count);

    docid_t getDocId(uint32_t pos) const;

    matchdoc::MatchDoc getMatchDoc(uint32_t pos) const;
    matchdoc::MatchDoc &getMatchDoc(uint32_t pos);

    matchdoc::MatchDoc stealMatchDoc(uint32_t pos);

    void stealMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVect);

    // todo
    // void setHashId(hashid_t pid);
    // hashid_t getHashId(uint32_t pos) const;

    void setClusterId(clusterid_t clusterId);
    void addMatchDoc(matchdoc::MatchDoc matchDoc);
    void insertMatchDoc(uint32_t pos, matchdoc::MatchDoc matchDoc);

    void setTotalMatchDocs(uint32_t totalMatchDocs);
    uint32_t totalMatchDocs() const;

    void setActualMatchDocs(uint32_t actualMatchDocs);
    uint32_t actualMatchDocs() const;

    void setMatchDocAllocator(const Ha3MatchDocAllocatorPtr &allocator) {
        _allocator = allocator;
    }
    const Ha3MatchDocAllocatorPtr &getMatchDocAllocator() const {
        return _allocator;
    }
    std::string toString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

    bool hasPrimaryKey() const;
    void setGlobalIdInfo(hashid_t hashId, versionid_t indexVersion, 
                         FullIndexVersion fullIndexVersion, uint32_t ip,
                         uint8_t phaseOneInfoFlag);
    std::vector<matchdoc::MatchDoc>& getMatchDocsVect() {
        return _matchDocs;
    }

    matchdoc::ReferenceBase* getRawPrimaryKeyRef() const;
    matchdoc::Reference<hashid_t> *getHashIdRef() const;
    matchdoc::Reference<clusterid_t> *getClusterIdRef() const;
    matchdoc::Reference<primarykey_t> *getPrimaryKey128Ref() const;
    matchdoc::Reference<uint64_t> *getPrimaryKey64Ref() const;
    matchdoc::Reference<FullIndexVersion> *getFullIndexVersionRef() const;
    matchdoc::Reference<versionid_t> *getIndexVersionRef() const;
    matchdoc::Reference<uint32_t> *getIpRef() const;

    void setSerializeLevel(uint8_t serializeLevel) {
        assert(SL_NONE < serializeLevel &&
               serializeLevel <= SL_MAX);
        _serializeLevel = serializeLevel;
    }
private:
    std::vector<matchdoc::MatchDoc> _matchDocs;
    Ha3MatchDocAllocatorPtr _allocator;
    uint32_t _totalMatchDocs;
    uint32_t _actualMatchDocs;
    // for searcher cache
    uint8_t _serializeLevel;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

