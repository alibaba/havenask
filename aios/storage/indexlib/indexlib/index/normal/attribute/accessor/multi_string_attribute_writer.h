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
#ifndef __INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H
#define __INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class MultiStringAttributeWriter : public VarNumAttributeWriter<char>
{
public:
    MultiStringAttributeWriter(const config::AttributeConfigPtr& attrConfig) : VarNumAttributeWriter<char>(attrConfig)
    {
    }
    ~MultiStringAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(multi_string);

public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const { return ft_string; }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new MultiStringAttributeWriter(attrConfig);
        }
    };

public:
    const AttributeSegmentReaderPtr CreateInMemReader() const override;
};

//////////////////////////////////////////////////////////////////

inline const AttributeSegmentReaderPtr MultiStringAttributeWriter::CreateInMemReader() const
{
    InMemVarNumAttributeReader<autil::MultiChar>* pReader = new InMemVarNumAttributeReader<autil::MultiChar>(
        mAccessor, mAttrConfig->GetCompressType(), mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount(),
        mAttrConfig->SupportNull());
    return AttributeSegmentReaderPtr(pReader);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H
