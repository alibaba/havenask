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

#include "matchdoc/ReferenceSet.h"
#include "matchdoc/Trait.h"

namespace matchdoc {

class FieldGroup;

class FieldGroupBuilder {
public:
    FieldGroupBuilder(const std::string &groupName, autil::mem_pool::Pool *pool);
    FieldGroupBuilder(const std::string &groupName, const std::shared_ptr<autil::mem_pool::Pool> &pool);
    ~FieldGroupBuilder();

public:
    ReferenceBase *declare(const ReferenceMeta &meta) { return declare(meta.name, meta); }
    ReferenceBase *declare(const std::string &name, const ReferenceMeta &meta);

    ReferenceBase *
    declareMount(const std::string &name, const ReferenceMeta &meta, uint32_t mountId, uint64_t mountOffset);

    template <typename T>
    Reference<T> *declare(const std::string &name,
                          bool needConstruct = ConstructTypeTraits<T>::NeedConstruct(),
                          uint32_t allocateSize = sizeof(T)) {
        allocateSize = std::max(allocateSize, (uint32_t)sizeof(T));
        auto meta = ReferenceMeta::make<T>(needConstruct, allocateSize);
        return dynamic_cast<Reference<T> *>(declare(name, meta));
    }

    template <typename T>
    Reference<T> *declareMount(const std::string &name,
                               uint32_t mountId,
                               uint64_t mountOffset,
                               bool needConstruct = ConstructTypeTraits<T>::NeedConstruct(),
                               uint32_t allocateSize = sizeof(T)) {
        auto meta = ReferenceMeta::make<T>(needConstruct, allocateSize);
        return dynamic_cast<Reference<T> *>(declareMount(name, meta, mountId, mountOffset));
    }

    template <typename T>
    std::unique_ptr<FieldGroup> fromBuffer(T *data, size_t count) {
        Reference<T> *ref = declare<T>(_groupName, false);
        if (ref == nullptr) {
            return nullptr;
        }
        return fromBuffer(reinterpret_cast<char *>(data), sizeof(T), count);
    }

    std::unique_ptr<FieldGroup> finalize();

    bool equals(const FieldGroupBuilder &other) const;

    bool dropField(const std::string &refName) { return _referenceSet->remove(refName); }
    bool renameField(const std::string &refName, const std::string &dstName) {
        return _referenceSet->rename(refName, dstName);
    }

    const ReferenceSet &getReferenceSet() const { return *_referenceSet; }

private:
    std::unique_ptr<FieldGroup> fromBuffer(char *data, size_t docSize, size_t count);

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    size_t _docSize = 0;
    std::string _groupName;
    std::unique_ptr<ReferenceSet> _referenceSet;
    std::map<uint32_t, uint32_t> _mountIdOffsets;
};

} // namespace matchdoc
