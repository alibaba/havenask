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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class StringAttributeWriter : public VarNumAttributeWriter<char>
{
public:
    StringAttributeWriter(const config::AttributeConfigPtr& attrConfig) : VarNumAttributeWriter<char>(attrConfig) {}

    ~StringAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(string);

    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const { return ft_string; }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new StringAttributeWriter(attrConfig);
        }
    };
};

DEFINE_SHARED_PTR(StringAttributeWriter);

///////////////////////////////////////////////////////
}} // namespace indexlib::index
