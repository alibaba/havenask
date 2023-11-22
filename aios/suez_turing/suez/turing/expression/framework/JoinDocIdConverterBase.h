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
#include <string>

#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

class DocIdWrapper {
public:
    DocIdWrapper(docid_t id = indexlib::UNINITIALIZED_DOCID) : _id(id) {}
    operator docid_t() const { return _id; }
    docid_t _id;
};

class JoinDocIdConverterBase {
public:
    JoinDocIdConverterBase(const std::string &joinAttrName, indexlib::partition::JoinDocidReaderBase *joinDocidReader)
        : _strongJoin(false)
        , _joinAttrName(joinAttrName)
        , _docIdRef(NULL)
        , _subDocIdRef(NULL)
        , _joinDocidReader(joinDocidReader) {}

    ~JoinDocIdConverterBase() { POOL_DELETE_CLASS(_joinDocidReader); }

private:
    JoinDocIdConverterBase(const JoinDocIdConverterBase &);
    JoinDocIdConverterBase &operator=(const JoinDocIdConverterBase &);

public:
    docid_t convert(matchdoc::MatchDoc matchDoc);

public:
    bool init(matchdoc::MatchDocAllocator *matchDocAllocator, bool isSubJoin = false) {
        assert(matchDocAllocator);
        if (_docIdRef != NULL) {
            return true;
        }
        std::string refName = BUILD_IN_JOIN_DOCID_REF_PREIX + _joinAttrName;
        if (isSubJoin) {
            _docIdRef = matchDocAllocator->declareSub<DocIdWrapper>(refName);
            _subDocIdRef = matchDocAllocator->getCurrentSubDocRef();
        } else {
            _docIdRef = matchDocAllocator->declare<DocIdWrapper>(refName);
        }
        return true;
    }
    matchdoc::Reference<DocIdWrapper> *getJoinDocIdRef() const { return _docIdRef; }
    void setStrongJoin(bool flag) { _strongJoin = flag; }
    bool isStrongJoin() const { return _strongJoin; }
    bool isSubJoin() const { return _subDocIdRef != NULL; }

public:
    // for test
    void setSubDocIdRef(matchdoc::Reference<matchdoc::MatchDoc> *ref) { _subDocIdRef = ref; }

protected:
    bool _strongJoin;
    std::string _joinAttrName;
    matchdoc::Reference<DocIdWrapper> *_docIdRef;
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocIdRef;
    indexlib::partition::JoinDocidReaderBase *_joinDocidReader;

private:
    AUTIL_LOG_DECLARE();
};

// Ha3_TYPEDEF_PTR(JoinDocIdConverterBase);
/////////////////////////////////////////////////
inline docid_t JoinDocIdConverterBase::convert(matchdoc::MatchDoc matchDoc) {
    docid_t joinDocId = indexlib::UNINITIALIZED_DOCID;

    if (_docIdRef != NULL) {
        joinDocId = *_docIdRef->getPointer(matchDoc);
        if (joinDocId != indexlib::UNINITIALIZED_DOCID) {
            return joinDocId;
        }
    }

    docid_t docId;
    if (_subDocIdRef != NULL) {
        auto subDoc = *_subDocIdRef->getPointer(matchDoc);
        docId = subDoc.getDocId();
    } else {
        docId = matchDoc.getDocId();
    }

    joinDocId = _joinDocidReader->GetAuxDocid(docId);
    if (joinDocId != INVALID_DOCID && _docIdRef != NULL) {
        _docIdRef->set(matchDoc, joinDocId);
    }
    return joinDocId;
}

} // namespace turing
} // namespace suez
