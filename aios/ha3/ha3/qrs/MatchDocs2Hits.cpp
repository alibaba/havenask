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
#include "ha3/qrs/MatchDocs2Hits.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "ha3/common/AttributeClause.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/Hits.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/PBResultFormatter.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortExprMeta.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/isearch.h"
#include "ha3/search/LayerMetas.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/VirtualAttrConvertor.h"
#include "turing_ops_util/variant/SortExprMeta.h"

using namespace std;
using namespace suez::turing;
using namespace isearch::common;
using namespace isearch::search;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, MatchDocs2Hits);

MatchDocs2Hits::MatchDocs2Hits() { 
    clear();
}

MatchDocs2Hits::~MatchDocs2Hits() { 
}

void MatchDocs2Hits::clear() {
    _hits = NULL;
    _hasPrimaryKey = false;
    _sortReferVec.clear();
    _attributes.clear();
    _hashIdRef = NULL;
    _clusterIdRef = NULL;
    _tracerRef = NULL;
    _primaryKey64Ref = NULL;
    _primaryKey128Ref = NULL;
    _fullVersionRef = NULL;
    _indexVersionRef = NULL;
    _ipRef = NULL;
    _rawPkRef = NULL;
}

void MatchDocs2Hits::extractNames(const matchdoc::ReferenceVector &referVec, 
                                  const string &prefix, vector<string> &nameVec) const
{
    size_t PREFIX_SIZE = prefix.size();
    nameVec.reserve(referVec.size());
    for (size_t i = 0; i < referVec.size(); ++i) {
        const string &name = referVec[i]->getName();
        assert(name.substr(0, PREFIX_SIZE) == prefix);
        nameVec.push_back(name.substr(PREFIX_SIZE));
    }
}

void MatchDocs2Hits::fillExtraDocIdentifier(const matchdoc::MatchDoc matchDoc,
        ExtraDocIdentifier &extId)
{
    if (_primaryKey64Ref) {
        extId.primaryKey = primarykey_t(_primaryKey64Ref->getReference(matchDoc));
    }
    if (_primaryKey128Ref) {
        extId.primaryKey = _primaryKey128Ref->getReference(matchDoc);
    }
    if (_fullVersionRef) {
        extId.fullIndexVersion = _fullVersionRef->getReference(matchDoc);
    }
    if (_indexVersionRef) {
        extId.indexVersion = _indexVersionRef->getReference(matchDoc);
    }
    if (_ipRef) {
        extId.ip = _ipRef->getReference(matchDoc);
    }
}

HitPtr MatchDocs2Hits::convertOne(const matchdoc::MatchDoc matchDoc) {
    Hit *hit = new Hit();
    GlobalIdentifier gid;

    gid.setDocId(matchDoc.getDocId());
    gid.setHashId(_hashIdRef->get(matchDoc));
    gid.setClusterId(_clusterIdRef->get(matchDoc));
    fillExtraDocIdentifier(matchDoc, gid.getExtraDocIdentifier());

    hit->setGlobalIdentifier(gid);
    hit->setHasPrimaryKeyFlag(_hasPrimaryKey);

    if (_rawPkRef) {
        string rawPk = _rawPkRef->toString(matchDoc);
        hit->setRawPk(rawPk);
    }

    for (size_t i = 0; i < _sortReferVec.size(); ++i) {
        auto ref = _sortReferVec[i];
        auto type = ref->getValueType().getBuiltinType();
        const string& value = ref->toString(matchDoc);
        hit->addSortExprValue(value, type);
        if (i == 0) {
            if (type != vt_string) {
                hit->setSortValue(autil::StringUtil::fromString<double>(value));
            } else {
                hit->setSortValue(0);
            }
        }
    }
    if (_tracerRef) {
        hit->setTracer(_tracerRef->getPointer(matchDoc));
    }
    for (size_t i = 0; i < _attributes.size(); ++i) {
        const auto &at = Ha3MatchDocAllocator::createAttributeItem(_attributes[i].second, matchDoc);
        hit->addAttribute(_attributes[i].first, at);
    }
    for (size_t i = 0; i < _userDataReferVec.size(); ++i) {
        const auto &at = Ha3MatchDocAllocator::createAttributeItem(_userDataReferVec[i], matchDoc);
        hit->addVariableValue(_userDataNames[i], at);
    }
    return HitPtr(hit);
}

bool MatchDocs2Hits::initAttributeReferences(const RequestPtr &requestPtr,
        const common::Ha3MatchDocAllocatorPtr &allocatorPtr)
{
    VirtualAttrConvertor convertor;
    auto virtualAttributeClause = requestPtr->getVirtualAttributeClause();
    if (virtualAttributeClause) {
        convertor.initVirtualAttrs(virtualAttributeClause->getVirtualAttributes());
    }
    AttributeClause *attrCluase = requestPtr->getAttributeClause();
    if (!attrCluase) {
        return true;
    }
    const set<string> &attrNames = attrCluase->getAttributeNames();
    for (set<string>::const_iterator it = attrNames.begin();
         it != attrNames.end(); ++it)
    {
        string attrName = convertor.nameToName(*it);
        auto ref = allocatorPtr->findReferenceWithoutType(attrName);
        if(ref == NULL) {
            AUTIL_LOG(WARN, "Can't find attribute reference, "
                    "virtual attribute name: %s, converted attribute name: %s", 
                    it->c_str(), attrName.c_str());
            return false;
        }
        _attributes.push_back(make_pair(*it, ref));
    }
    
    return true;
}

void MatchDocs2Hits::initUserData(const common::Ha3MatchDocAllocatorPtr &allocatorPtr) {
    _userDataReferVec = allocatorPtr->getAllNeedSerializeReferences(
            SL_VARIABLE);
    extractNames(_userDataReferVec, PROVIDER_VAR_NAME_PREFIX, _userDataNames);
}

void MatchDocs2Hits::initIdentifiers(const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                                     const MatchDocs *matchDocs) {
    _tracerRef = allocatorPtr->findReference<Tracer>(RANK_TRACER_NAME);
    _hashIdRef = matchDocs->getHashIdRef();
    assert(_hashIdRef);
    _clusterIdRef = matchDocs->getClusterIdRef();
    assert(_clusterIdRef);
    _primaryKey64Ref = matchDocs->getPrimaryKey64Ref();
    _primaryKey128Ref = matchDocs->getPrimaryKey128Ref();
    _fullVersionRef = matchDocs->getFullIndexVersionRef();
    _indexVersionRef = matchDocs->getIndexVersionRef();
    _ipRef = matchDocs->getIpRef();
    _rawPkRef = matchDocs->getRawPrimaryKeyRef();
}

Hits* MatchDocs2Hits::convert(const MatchDocs *matchDocs, const RequestPtr &requestPtr, 
                              const vector<common::SortExprMeta>& sortExprMetaVec, 
                              ErrorResult& errResult,
                              const std::vector<std::string> &clusterList) 
{
    clear();
    if (!requestPtr) {
        return NULL;
    }

    _hits = new Hits();

    if (!sortExprMetaVec.empty()) {
        _hits->setSortAscendFlag(sortExprMetaVec[0].sortFlag);
    }
    
    if (!matchDocs || matchDocs->size() == 0) {
        return _hits;
    }

    _hasPrimaryKey = matchDocs->hasPrimaryKey();

    _sortReferVec.clear();
    for (vector<common::SortExprMeta>::const_iterator it = sortExprMetaVec.begin(); 
         it != sortExprMetaVec.end(); ++it)
    {
        _sortReferVec.push_back(it->sortRef);
    }

    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    if (allocatorPtr) {
        if (!initAttributeReferences(requestPtr, allocatorPtr)) {
            DELETE_AND_SET_NULL(_hits);
            errResult.resetError(ERROR_QRS_ATTRIBUTE_CONVERT_RESULT_NULL);
            return NULL;            
        }
        initUserData(allocatorPtr);
        initIdentifiers(allocatorPtr, matchDocs);
    }

    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    uint32_t startOffset = configClause->getStartOffset();
    uint32_t hitCount = configClause->getHitCount();
    uint32_t matchDocsSize = matchDocs->size();

    if (matchDocsSize > hitCount + startOffset) {
        matchDocsSize = hitCount + startOffset;
    }

    if (startOffset < matchDocsSize) {
        _hits->reserve(matchDocsSize - startOffset);
    }
    
    for (uint32_t i = startOffset; i < matchDocsSize; ++i) {
        const auto matchDoc = matchDocs->getMatchDoc(i);
        _hits->insertHit(i - startOffset, convertOne(matchDoc));
    }

    size_t clusterSize = clusterList.size();
    for (size_t i = 0; i < _hits->size(); ++i) {
        const HitPtr &hitPtr = _hits->getHit(i);
        clusterid_t clusterId = hitPtr->getClusterId();
        if ((size_t)clusterId < clusterSize) {
            const string &clusterName = clusterList[clusterId];
            hitPtr->setClusterName(clusterName);
        } else {
            AUTIL_LOG(ERROR, "invalid cluster id [%d], cluster size [%d]",
                    (int32_t)clusterId, (int32_t)clusterSize);
        }
    }

    return _hits;
}

} // namespace qrs
} // namespace isearch
