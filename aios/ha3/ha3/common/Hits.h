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
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "indexlib/indexlib.h"

#include "ha3/common/Hit.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace common {
typedef std::map<std::string, std::string> MetaHit;
typedef std::map<std::string, MetaHit> MetaHitMap;
typedef std::map<HitPtr, void *> HitSortMap;
typedef std::map<int64_t, config::HitSummarySchema *> HitSummarySchemaMap;

class Hits
{
public:
    const static uint32_t DEFAULT_SIZE = 100;
    const static uint32_t INVALID_POS = 0xFFFFFFFF;
public:
    Hits() : _totalHits(0), _hasSummary(false), _noSummary(false)
           , _independentQuery(false), _sortAscendFlag(false)
    {}
    ~Hits() {
        clearHitSummarySchemas();
    }
public:
    uint32_t size() const;

    void setTotalHits(uint32_t totalHits);
    uint32_t totalHits() const;

    docid_t getDocId(uint32_t pos) const;
    bool getHashId(uint32_t pos, hashid_t &hashid) const;

    const HitPtr &getHit(uint32_t pos) const;
    void addHit(HitPtr hitPtr);
    void insertHit(uint32_t pos, HitPtr hitPtr);
    void deleteHit(uint32_t pos);
    const std::vector<HitPtr>& getHitVec() const {return _hits;}
    void setHitVec(const std::vector<common::HitPtr> &hitVec) {_hits = hitVec;}

    void addHitSummarySchema(config::HitSummarySchema *hitSummarySchema) {
        assert(hitSummarySchema);
        int64_t signature = hitSummarySchema->getSignature();
        if (_hitSummarySchemas.find(signature) == _hitSummarySchemas.end()) {
            _hitSummarySchemas[signature] = hitSummarySchema;
        } else {
            delete hitSummarySchema;
        }
    }

    HitSummarySchemaMap &getHitSummarySchemaMap() { return _hitSummarySchemas; }    

    void summarySchemaToSignature();

    void setClusterInfo(const std::string &clusterName, clusterid_t clusterId);

    void setSummaryFilled() {_hasSummary = true;}
    bool hasSummary() const {return _hasSummary;}

    void setNoSummary(bool noSummary) {_noSummary = noSummary;}
    bool isNoSummary() const {return _noSummary;}

    void setIndependentQuery(bool independentQuery) { _independentQuery = independentQuery; }
    bool isIndependentQuery() const  { return _independentQuery; }

    /** these interfaces are for visit 'MetaHit', which is insert by 'QrsProcessor'. */
    const MetaHitMap& getMetaHitMap() const {return _metaHitMap;}
    size_t getMetaHitCount() const {return _metaHitMap.size();}
    MetaHit& getMetaHit(const std::string &metaHitKey) {
        return _metaHitMap[metaHitKey];
    }
    const MetaHit& getMetaHit(const std::string &metaHitKey) const {
        MetaHitMap::const_iterator it = _metaHitMap.find(metaHitKey);
        if (it != _metaHitMap.end()) {
            return it->second;
        }
        return NULL_META_HIT;
    }
    void addMetaHit(const std::string &metaHitKey, const MetaHit& metaHit) {
        _metaHitMap[metaHitKey] = metaHit;
    }

    void reserve(uint32_t size);

    bool stealAndMergeHits(Hits &other, bool doDedup = false);
    size_t stealSummary(const std::vector<Hits*> &hitsVec);

    void sortHits();
    void setSortAscendFlag(bool flag) {_sortAscendFlag = flag;}

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

public:
    // for test
    void adjustHitSummarySchemaInHit();
    bool getSortAscendFlag() {return _sortAscendFlag;}
private:
    typedef bool (*HitPtrCompare) (const HitPtr&, const HitPtr&);
    void stealHitSummarySchemas(Hits &hits);
    void clearHitSummarySchemas();
private:
    std::vector<HitPtr> _hits;
    HitSummarySchemaMap _hitSummarySchemas;
    uint32_t _totalHits;
    bool _hasSummary;
    bool _noSummary; // used by qrs
    bool _independentQuery;
    bool _sortAscendFlag;
private:
    static const MetaHit NULL_META_HIT; 
private:
    MetaHitMap _metaHitMap;
private:
    friend class HitsTest;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

