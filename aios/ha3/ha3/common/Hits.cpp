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
#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Hits.h"
#include "ha3/common/SummaryHit.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace autil::legacy;
using namespace autil::mem_pool;
using namespace std;

using namespace isearch::util;
using namespace isearch::config;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, Hits);

const MetaHit Hits::NULL_META_HIT = MetaHit();

struct HitSortFunction {
    HitSortFunction(bool flag) : _flag(flag){}
    bool _flag;
    bool operator () (const HitPtr &hit1, const HitPtr &hit2) const {
        return _flag
            ? hit1->lessThan(hit2)
            : hit2->lessThan(hit1);
    };
};


uint32_t Hits::size() const {
    return _hits.size();
}

void Hits::setTotalHits(uint32_t totalHits) {
    _totalHits = totalHits;
}

uint32_t Hits::totalHits() const {
    return _totalHits;
}

docid_t Hits::getDocId(uint32_t pos) const {
    if ( pos >= _hits.size()) {
        return INVALID_DOCID;
    } else {
        return _hits[pos]->getDocId();
    }
}

bool Hits::getHashId(uint32_t pos, hashid_t &hashid) const {
    if ( pos >= _hits.size()) {
        return false;
    } else {
        hashid = _hits[pos]->getHashId();
        return true;
    }
}

const HitPtr &Hits::getHit(uint32_t pos) const {
    if (pos >= _hits.size()) {
        static HitPtr EMPTY_HIT;
        return EMPTY_HIT;
    }
    return _hits[pos];
}

void Hits::addHit(HitPtr hitPtr) {
    _hits.push_back(hitPtr);
}

void Hits::insertHit(uint32_t pos, HitPtr hitPtr) {
    vector<HitPtr>::iterator it = _hits.begin();
    _hits.insert(it + pos, hitPtr);
}
void Hits::deleteHit(uint32_t pos) {
    vector<HitPtr>::iterator it = _hits.begin();
    _hits.erase(it + pos);
}

void Hits::reserve(uint32_t size) {
    _hits.reserve(size);
}

void Hits::stealHitSummarySchemas(Hits &hits) {
    for (HitSummarySchemaMap::const_iterator it = hits._hitSummarySchemas.begin();
         it != hits._hitSummarySchemas.end(); ++it)
    {
        int64_t signature = it->first;
        map<int64_t, HitSummarySchema *>::const_iterator mIt = _hitSummarySchemas.find(signature);
        if (mIt == _hitSummarySchemas.end()) {
            _hitSummarySchemas[signature] = it->second;
        } else {
            delete it->second;
        }
    }
    hits._hitSummarySchemas.clear();
}

size_t Hits::stealSummary(const vector<Hits*> &hitsVec) {
    uint32_t returnedHitCount = 0;
    for (size_t i = 0; i < hitsVec.size(); ++i) {
        stealHitSummarySchemas(*hitsVec[i]);
        for (vector<HitPtr>::iterator it = hitsVec[i]->_hits.begin();
             it != hitsVec[i]->_hits.end(); ++it)
        {
            HitPtr &hitPtr = *it;
            if (!hitPtr->hasSummary()) {
                continue;
            }
            uint32_t pos = hitPtr->getPosition();
            assert(pos < _hits.size());
            if (!_hits[pos]->hasSummary()) {
                _hits[pos]->stealSummary(*hitPtr);
                ++returnedHitCount;
            }
        }
        _hasSummary |= hitsVec[i]->hasSummary();
    }
    return _hits.size() - returnedHitCount;
}

bool Hits::stealAndMergeHits(Hits &other, bool doDedup) {
    typedef std::map<primarykey_t, size_t> ResultMap;
    ResultMap resultMap;

    size_t hitSize = _hits.size();
    size_t otherHitSize = other._hits.size();
    _hits.reserve(hitSize + otherHitSize);

    if (doDedup) {
        for (size_t i = 0; i < hitSize; ++i) {
            const HitPtr &hitPtr = getHit(i);
            if (hitPtr->hasPrimaryKey()) {
                resultMap.insert(std::make_pair(hitPtr->getPrimaryKey(), i));
            }
        }
    }

    stealHitSummarySchemas(other);

    for (size_t i = 0; i < otherHitSize; ++i) {
        const HitPtr &hitPtr = other._hits[i];
        if (doDedup && hitPtr->hasPrimaryKey()) {
            primarykey_t primaryKey = hitPtr->getPrimaryKey();
            ResultMap::iterator it = resultMap.find(primaryKey);

            if (it == resultMap.end()) {
                resultMap.insert(std::make_pair(primaryKey, _hits.size()));
            } else {
                if (_hits[it->second]->getFullIndexVersion() < hitPtr->getFullIndexVersion()) {
                    _hits[it->second] = hitPtr;
                }
                continue;
            }
        }
        _hits.push_back(hitPtr);
    }

    other._hits.clear();
    _totalHits += other.totalHits();
    return true;
}

void Hits::sortHits() {
    HitSortFunction function(_sortAscendFlag);
    sort(_hits.begin(), _hits.end(), function);
}

void Hits::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_totalHits);
    dataBuffer.write(_hasSummary);

    uint32_t size = _hitSummarySchemas.size();
    dataBuffer.write(size);
    for (HitSummarySchemaMap::const_iterator it = _hitSummarySchemas.begin();
         it != _hitSummarySchemas.end(); ++it)
    {
        assert(it->second);
        dataBuffer.write(*it->second);
    }

    uint32_t hitCount = _hits.size();
    dataBuffer.write(hitCount);
    for (vector<HitPtr>::const_iterator it = _hits.begin();
         it != _hits.end(); ++it)
    {
        dataBuffer.write(**it);
    }
}

void Hits::deserialize(autil::DataBuffer &dataBuffer, Pool *pool) {
    clearHitSummarySchemas();
    dataBuffer.read(_totalHits);
    dataBuffer.read(_hasSummary);
    uint32_t size = 0;
    dataBuffer.read(size);
    for (uint32_t i = 0; i < size; ++i) {
        HitSummarySchema *hitSummarySchema = new HitSummarySchema;
        dataBuffer.read(*hitSummarySchema);
        _hitSummarySchemas[hitSummarySchema->getSignature()] = hitSummarySchema;
    }

    uint32_t hitCount = 0;
    dataBuffer.read(hitCount);
    _hits.resize(hitCount);
    for (uint32_t i = 0; i < hitCount; i++)
    {
        _hits[i].reset(new Hit());
        _hits[i]->deserialize(dataBuffer, pool);
    }
}

void Hits::setClusterInfo(const string &clusterName, clusterid_t clusterId) {
    for (vector<HitPtr>::iterator it = _hits.begin();
         it != _hits.end(); ++it)
    {
        (*it)->setClusterName(clusterName);
        (*it)->setClusterId(clusterId);
    }
}

void Hits::summarySchemaToSignature() {
    for (vector<HitPtr>::iterator it = _hits.begin();
         it != _hits.end(); ++it)
    {
        SummaryHit *summaryHit = (*it)->getSummaryHit();
        if (summaryHit) {
            summaryHit->summarySchemaToSignature();
        }
    }
}

void Hits::adjustHitSummarySchemaInHit() {
    for (vector<HitPtr>::iterator it = _hits.begin();
         it != _hits.end(); ++it)
    {
        SummaryHit *summaryHit = (*it)->getSummaryHit();
        if (!summaryHit) {
            continue;
        }
        int64_t signature = summaryHit->getSignature();
        summaryHit->setHitSummarySchema(_hitSummarySchemas[signature]);
    }
}

void Hits::clearHitSummarySchemas() {
    for (HitSummarySchemaMap::iterator it = _hitSummarySchemas.begin();
         it != _hitSummarySchemas.end(); ++it)
    {
        delete it->second;
    }
    _hitSummarySchemas.clear();
}

} // namespace common
} // namespace isearch
