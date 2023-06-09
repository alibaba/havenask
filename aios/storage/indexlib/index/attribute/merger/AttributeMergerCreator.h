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

#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/merger/AttributeMerger.h"

namespace indexlibv2::index {

#define DECLARE_ATTRIBUTE_MERGER_CREATOR_CLASS(classname, indextype)                                                   \
    class classname##Creator : public AttributeMergerCreator                                                           \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
        std::unique_ptr<AttributeMerger> Create(bool isUniqEncoded) const override                                     \
        {                                                                                                              \
            return std::make_unique<classname>();                                                                      \
        }                                                                                                              \
    };

class AttributeMergerCreator
{
public:
    AttributeMergerCreator() = default;
    virtual ~AttributeMergerCreator() = default;

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual std::unique_ptr<AttributeMerger> Create(bool isUniqEncoded) const = 0;
};

} // namespace indexlibv2::index
