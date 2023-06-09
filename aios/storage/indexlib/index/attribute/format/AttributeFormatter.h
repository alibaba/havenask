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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/CompressTypeOption.h"

namespace indexlibv2::index {
class AttributeConvertor;
class AttributeFormatter : private autil::NoCopyable
{
public:
    AttributeFormatter() = default;
    virtual ~AttributeFormatter() = default;

public:
    void SetAttrConvertor(const std::shared_ptr<AttributeConvertor>& attrConvertor);
    const std::shared_ptr<AttributeConvertor>& GetAttrConvertor() const { return _attrConvertor; }

    virtual uint32_t GetDataLen(int64_t docCount) const = 0;

public:
    virtual void TEST_Get(docid_t docId, const uint8_t*& buffer, std::string& attributeValue, bool& isNull) const
    {
        assert(false);
    }

protected:
    std::shared_ptr<AttributeConvertor> _attrConvertor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
