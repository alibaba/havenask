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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/MultiValueAttributeReader.h"

namespace indexlib::index {

class SectionDataReader : public indexlibv2::index::MultiValueAttributeReader<char>
{
public:
    SectionDataReader(indexlibv2::config::SortPattern sortType)
        : indexlibv2::index::MultiValueAttributeReader<char>(sortType)
    {
    }
    ~SectionDataReader() = default;

public:
    bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const override;
    AttrType GetType() const override;
    bool IsMultiValue() const override;

protected:
    Status DoOpen(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                  const std::vector<IndexerInfo>& indexers) override;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////
inline bool SectionDataReader::Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!indexlibv2::index::MultiValueAttributeReader<char>::Read(docId, value, pool)) {
        return false;
    }
    attrValue = std::string(value.data(), value.size());
    return true;
}

inline AttrType SectionDataReader::GetType() const { return AT_STRING; }

inline bool SectionDataReader::IsMultiValue() const { return false; }

} // namespace indexlib::index
