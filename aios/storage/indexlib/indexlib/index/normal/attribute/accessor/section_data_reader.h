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
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class SectionDataReader : public VarNumAttributeReader<char>
{
public:
    using VarNumAttributeReader<char>::Read;

public:
    SectionDataReader();
    ~SectionDataReader();

protected:
    file_system::DirectoryPtr GetAttributeDirectory(const index_base::SegmentData& segmentData,
                                                    const config::AttributeConfigPtr& attrConfig) const override
    {
        std::string indexName =
            config::SectionAttributeConfig::SectionAttributeNameToIndexName(attrConfig->GetAttrName());
        return segmentData.GetSectionAttributeDirectory(indexName, true);
    }

    void InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& segIter) override;

public:
    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool = NULL) const override;

    AttrType GetType() const override;
    bool IsMultiValue() const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionDataReader);

//////////////////////////////////////////////////////
inline bool SectionDataReader::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!VarNumAttributeReader<char>::Read(docId, value, pool)) {
        return false;
    }
    attrValue = std::string(value.data(), value.size());
    return true;
}

inline AttrType SectionDataReader::GetType() const { return AT_STRING; }

inline bool SectionDataReader::IsMultiValue() const { return false; }
}}} // namespace indexlib::index::legacy
