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
#include <memory>

#include "indexlib/index/attribute/merger/AttributeMergerCreator.h"
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"
#include "indexlib/index/attribute/merger/UniqEncodedMultiValueAttributeMerger.h"

namespace indexlibv2::index {

#define DECLARE_MULTI_VALUE_ATTRIBUTE_MERGER_CREATOR_CLASS(classname, type, indextype)                                 \
    class classname##Creator : public MultiValueAttributeMergerCreator<type>                                           \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
    };

template <typename T>
class MultiValueAttributeMergerCreator : public AttributeMergerCreator
{
public:
    MultiValueAttributeMergerCreator() = default;
    virtual ~MultiValueAttributeMergerCreator() = default;

public:
    FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

    std::unique_ptr<AttributeMerger> Create(bool isUniqEncoded) const override
    {
        if (isUniqEncoded) {
            return std::make_unique<UniqEncodedMultiValueAttributeMerger<T>>();
        } else {
            return std::make_unique<MultiValueAttributeMerger<T>>();
        }
    }
};

} // namespace indexlibv2::index
