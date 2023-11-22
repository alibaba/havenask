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
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ReferenceSet.h"
#include "matchdoc/ReferenceTypesWrapper.h"
#include "matchdoc/VectorStorage.h"

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace matchdoc {
typedef std::unordered_set<std::string> ReferenceNameSet;

class FieldGroup {
public:
    FieldGroup(const std::string &groupName,
               std::shared_ptr<VectorStorage> storage,
               std::unique_ptr<ReferenceSet> refSet);
    ~FieldGroup();

private:
    FieldGroup(const FieldGroup &);
    FieldGroup &operator=(const FieldGroup &);

public:
    static std::unique_ptr<FieldGroup>
    make(const std::string &groupName, std::shared_ptr<VectorStorage> storage, std::unique_ptr<ReferenceSet> refSet);

    static std::unique_ptr<FieldGroup> makeSingleFieldGroup(matchdoc::ReferenceBase *ref);

public:
    template <typename T>
    Reference<T> *findReference(const std::string &name) const {
        return dynamic_cast<Reference<T> *>(findReferenceWithoutType(name));
    }
    ReferenceBase *findReferenceWithoutType(const std::string &name) const { return _referenceSet->get(name); }

    bool dropField(const std::string &name) { return _referenceSet->remove(name); }
    bool renameField(const std::string &name, const std::string &dstName) {
        return _referenceSet->rename(name, dstName);
    }

    void growToSize(uint32_t size);

    void constructDoc(const MatchDoc &doc);
    void constructDocs(const std::vector<MatchDoc> &docs);
    void destructDoc(const MatchDoc &doc);
    void destructDoc(const MatchDoc *docs, uint32_t count);
    void cloneDoc(const MatchDoc &newDoc, const MatchDoc &cloneDoc);
    // clone match doc from other allocator
    void cloneDoc(FieldGroup *other, const MatchDoc &newDoc, const MatchDoc &cloneDoc);

    uint32_t getDocSize() const { return _storage->getDocSize(); }

    void clearContent() { _storage->reset(); }

    void resetSerializeLevelAndAlias(uint8_t level = 0);
    void resetSerializeLevel(uint8_t level);

    bool needSerialize(uint8_t serializeLevel) const;

    bool isSame(const FieldGroup &other) const;

    const std::string &getGroupName() const { return _groupName; }

    void init();

public:
    const ReferenceSet &getReferenceSet() const { return *_referenceSet; }
    ReferenceSet &getReferenceSet() { return *_referenceSet; }
    const ReferenceMap &getReferenceMap() const { return _referenceSet->referenceMap; }
    const ReferenceVector &getReferenceVec() const { return _referenceSet->referenceVec; }
    void getAllReferenceNames(ReferenceNameSet &names) const {
        _referenceSet->for_each([&names](auto r) { names.insert(r->getName()); });
    }
    void appendGroup(FieldGroup *other);
    void truncateGroup(uint32_t size);
    void swapDocStorage(FieldGroup &other);

    bool needConstruct() const { return _needConstruct; }
    bool needDestruct() const { return _needDestruct; }

private:
    void maybeResetMount(const MatchDoc &doc);
    void maybeResetMount(const MatchDoc *docs, size_t count);

private:
    std::string _groupName;
    std::shared_ptr<VectorStorage> _storage;
    std::unique_ptr<ReferenceSet> _referenceSet;
    bool _needConstruct = false;
    bool _needDestruct = false;
    std::vector<uint32_t> _mountOffsets; // TODO: remove

private:
    AUTIL_LOG_DECLARE();
};

inline void FieldGroup::constructDoc(const MatchDoc &doc) {
    if (!_needConstruct) {
        return;
    }

    maybeResetMount(doc);

    _referenceSet->for_each([&doc](auto r) {
        if (!r->isMount() && r->needConstructMatchDoc()) {
            r->construct(doc);
        }
    });
}

inline void FieldGroup::constructDocs(const std::vector<MatchDoc> &docs) {
    if (!_needConstruct) {
        return;
    }

    maybeResetMount(docs.data(), docs.size());

    _referenceSet->for_each([&docs](auto ref) {
        if (!ref->isMount() && ref->needConstructMatchDoc()) {
            ref->constructDocs(docs);
        }
    });
}

inline void FieldGroup::destructDoc(const MatchDoc &doc) {
    if (!_needDestruct) {
        return;
    }

    maybeResetMount(doc);

    _referenceSet->for_each([&doc](auto ref) {
        if (!ref->isMount() && ref->needDestructMatchDoc()) {
            ref->destruct(doc);
        }
    });
}

inline void FieldGroup::destructDoc(const MatchDoc *docs, uint32_t count) {
    if (!_needDestruct) {
        return;
    }

    maybeResetMount(docs, count);

    _referenceSet->for_each([&](auto ref) {
        if (!ref->isMount() && ref->needDestructMatchDoc()) {
            for (uint32_t i = 0; i < count; i++) {
                ref->destruct(docs[i]);
            }
        }
    });
}

inline void FieldGroup::cloneDoc(const MatchDoc &newDoc, const MatchDoc &cloneDoc) {
    _referenceSet->for_each([&](auto ref) { ref->cloneConstruct(newDoc, cloneDoc); });
}

inline void FieldGroup::growToSize(uint32_t size) { _storage->growToSize(size); }

} // namespace matchdoc
