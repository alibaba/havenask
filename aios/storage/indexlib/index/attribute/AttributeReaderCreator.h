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
#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/attribute/AttributeReader.h"

namespace indexlibv2::index {

#define DECLARE_ATTRIBUTE_READER_CREATOR_CLASS(classname, indextype)                                                   \
    class classname##Creator : public AttributeReaderCreator                                                           \
    {                                                                                                                  \
    public:                                                                                                            \
        FieldType GetAttributeType() const override { return indextype; }                                              \
        std::unique_ptr<AttributeReader> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,              \
                                                const IndexReaderParameter& indexReaderParam) const override           \
        {                                                                                                              \
            return std::make_unique<classname>(GetSortPattern(indexConfig, indexReaderParam));                         \
        }                                                                                                              \
    };

class AttributeReaderCreator
{
public:
    AttributeReaderCreator() = default;
    virtual ~AttributeReaderCreator() = default;

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual std::unique_ptr<AttributeReader> Create(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexReaderParameter& indexReaderParam) const = 0;

protected:
    config::SortPattern GetSortPattern(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const IndexReaderParameter& indexReaderParam) const
    {
        auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
        assert(nullptr != attrConfig);
        if (indexReaderParam.sortPatternFunc) {
            return indexReaderParam.sortPatternFunc(attrConfig->GetAttrName());
        }
        return config::sp_nosort;
    }
};

} // namespace indexlibv2::index
