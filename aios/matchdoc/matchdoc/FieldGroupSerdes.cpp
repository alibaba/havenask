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
#include "matchdoc/FieldGroupSerdes.h"

#include "autil/Log.h"
#include "matchdoc/FieldGroupBuilder.h"

namespace matchdoc {
AUTIL_DECLARE_AND_SETUP_LOGGER(matchdoc, FieldGroupSerdes);

void FieldGroupSerdes::serializeMeta(FieldGroup *fieldGroup,
                                     autil::DataBuffer &dataBuffer,
                                     uint8_t serializeLevel,
                                     bool alphabetOrder) {
    dataBuffer.write(fieldGroup->getGroupName());
    auto &referenceSet = fieldGroup->getReferenceSet();
    if (alphabetOrder) {
        referenceSet.sort();
    }
    size_t count = 0;
    referenceSet.for_each([&](auto r) { count += (r->getSerializeLevel() >= serializeLevel ? 1 : 0); });
    dataBuffer.write(count);

    referenceSet.for_each([&](auto r) {
        if (r->getSerializeLevel() >= serializeLevel) {
            dataBuffer.write(r->getReferenceMeta());
        }
    });
}

void FieldGroupSerdes::serializeData(const FieldGroup *fieldGroup,
                                     autil::DataBuffer &dataBuffer,
                                     const MatchDoc &doc,
                                     uint8_t serializeLevel) {
    const auto &referenceSet = fieldGroup->getReferenceSet();
    referenceSet.for_each([&](auto ref) {
        if (ref->getSerializeLevel() >= serializeLevel) {
            ref->serialize(doc, dataBuffer);
        }
    });
}

std::unique_ptr<FieldGroup> FieldGroupSerdes::deserializeMeta(autil::mem_pool::Pool *pool,
                                                              autil::DataBuffer &dataBuffer) {
    std::string name;
    dataBuffer.read(name);

    auto builder = std::make_unique<FieldGroupBuilder>(name, pool);
    size_t count = 0;
    dataBuffer.read(count);
    for (size_t i = 0; i < count; ++i) {
        ReferenceMeta meta;
        dataBuffer.read(meta, pool);
        auto ref = builder->declare(meta);
        if (!ref) {
            AUTIL_LOG(ERROR, "deserialize %s failed, type: %s", meta.name.c_str(), meta.vt.c_str());
            return nullptr;
        }
    }
    return builder->finalize();
}

void FieldGroupSerdes::deserializeData(FieldGroup *fieldGroup, autil::DataBuffer &dataBuffer, const MatchDoc &doc) {
    auto &referenceSet = fieldGroup->getReferenceSet();
    referenceSet.for_each([&](auto ref) { ref->deserialize(doc, dataBuffer, ref->mutableStorage()->getPool()); });
}

} // namespace matchdoc
