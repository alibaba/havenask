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
#ifndef __INDEXLIB_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_ATTRIBUTE_FORMATTER_H

#include <memory>

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

DECLARE_REFERENCE_CLASS(file_system, FileStream);

namespace indexlib { namespace index {

class AttributeFormatter
{
public:
    AttributeFormatter();
    virtual ~AttributeFormatter();

public:
    virtual void Init(config::CompressTypeOption compressType = config::CompressTypeOption(),
                      bool supportNull = false) = 0;

    virtual void Set(docid_t docId, const autil::StringView& attributeValue,
                     const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull = false) = 0;

    virtual void Set(const autil::StringView& attributeValue, uint8_t* oneDocBaseAddr, bool isNull = false)
    {
        assert(false);
    }

    virtual bool Reset(docid_t docId, const autil::StringView& attributeValue,
                       const util::ByteAlignedSliceArrayPtr& fixedData, bool isNull = false) = 0;

    void SetAttrConvertor(const common::AttributeConvertorPtr& attrConvertor);
    const common::AttributeConvertorPtr& GetAttrConvertor() const { return mAttrConvertor; }

    virtual uint32_t GetDataLen(int64_t docCount) const = 0;

public:
    // for test
    virtual void Get(docid_t docId, const util::ByteAlignedSliceArrayPtr& fixedData, std::string& attributeValue,
                     bool& isNull) const
    {
        assert(false);
    }

    virtual void Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue, bool& isNull) const
    {
        assert(false);
    }

protected:
    common::AttributeConvertorPtr mAttrConvertor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeFormatter);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_FORMATTER_H
