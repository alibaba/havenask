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

#include "autil/DataBuffer.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDoc.h"

namespace matchdoc {

/**
 * serialize and deserialize for FieldGroup
 */
class FieldGroupSerdes {
public:
    static void serializeMeta(FieldGroup *fieldGroup,
                              autil::DataBuffer &dataBuffer,
                              uint8_t serializeLevel,
                              bool alphabetOrder = true);
    static void serializeData(const FieldGroup *fieldGroup,
                              autil::DataBuffer &dataBuffer,
                              const MatchDoc &doc,
                              uint8_t serializeLevel);

    static std::unique_ptr<FieldGroup> deserializeMeta(autil::mem_pool::Pool *pool, autil::DataBuffer &dataBuffer);
    static void deserializeData(FieldGroup *fieldGroup, autil::DataBuffer &dataBuffer, const MatchDoc &doc);
};

} // namespace matchdoc
