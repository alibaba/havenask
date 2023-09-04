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

#include <string>

#include "indexlib/indexlib.h"
#include "matchdoc/ValueType.h"

namespace suez {
namespace turing {

class TypeTransformer {
public:
    TypeTransformer();
    ~TypeTransformer();

public:
    static matchdoc::BuiltinType transform(FieldType fieldType);
    static FieldType transform(std::string fieldTypeStr);
    static FieldType transform(matchdoc::BuiltinType builtinType);
};

} // namespace turing
} // namespace suez
