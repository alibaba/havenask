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

#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"

namespace suez {
namespace turing {

class DefaultDocIdAccessor {
public:
    DefaultDocIdAccessor() {}
    ~DefaultDocIdAccessor() {}

public:
    inline docid_t getDocId(matchdoc::MatchDoc matchDoc) const { return matchDoc.getDocId(); }
    inline bool operator==(const DefaultDocIdAccessor &other) const { return true; }
};

class JoinDocIdAccessor {
public:
    JoinDocIdAccessor(JoinDocIdConverterBase *joinDocIdConverter = NULL) : _converter(joinDocIdConverter) {}
    ~JoinDocIdAccessor() {}

public:
    inline docid_t getDocId(matchdoc::MatchDoc matchDoc) const {
        assert(_converter != NULL);
        return _converter->convert(matchDoc);
    }
    inline bool operator==(const JoinDocIdAccessor &other) const { return _converter == other._converter; }

private:
    JoinDocIdConverterBase *_converter;
};

class SubDocIdAccessor {
public:
    SubDocIdAccessor(matchdoc::Reference<matchdoc::MatchDoc> *subDocRef) : _subDocRef(subDocRef) {}
    ~SubDocIdAccessor() {}

public:
    inline docid_t getDocId(matchdoc::MatchDoc matchDoc) const {
        auto subDoc = *_subDocRef->getPointer(matchDoc);
        return subDoc.getDocId();
    }
    inline bool operator==(const SubDocIdAccessor &other) const { return _subDocRef == other._subDocRef; }

private:
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocRef;
};

} // namespace turing
} // namespace suez
