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
#include "indexlib/base/Types.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/section_attribute/SectionDataReader.h"

namespace indexlib::index {
class InDocSectionMeta;
class SectionAttributeReaderImpl : public SectionAttributeReader
{
public:
    SectionAttributeReaderImpl(indexlibv2::config::SortPattern sortType) : _sortType(sortType) {}
    virtual ~SectionAttributeReaderImpl() = default;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                        const indexlibv2::framework::TabletData* tabletData);

    // virtual for test
    virtual SectionAttributeReader* Clone() const;

    bool HasFieldId() const { return _indexConfig->GetSectionAttributeConfig()->HasFieldId(); }
    bool HasSectionWeight() const { return _indexConfig->GetSectionAttributeConfig()->HasSectionWeight(); }
    fieldid_t GetFieldId(int32_t fieldIdxInPack) const override { return _indexConfig->GetFieldId(fieldIdxInPack); }
    std::shared_ptr<InDocSectionMeta> GetSection(docid_t docId) const override;
    virtual int32_t Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const;

private:
    bool ReadAndDecodeSectionData(docid_t docId, uint8_t* buffer, uint32_t bufLen, autil::mem_pool::Pool* pool) const;

protected:
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> _indexConfig;

private:
    std::shared_ptr<SectionDataReader> _sectionDataReader;
    std::shared_ptr<SectionAttributeFormatter> _formatter;
    indexlibv2::config::SortPattern _sortType;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////

inline bool SectionAttributeReaderImpl::ReadAndDecodeSectionData(docid_t docId, uint8_t* buffer, uint32_t bufLen,
                                                                 autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!_sectionDataReader->MultiValueAttributeReader<char>::Read(docId, value, pool)) {
        AUTIL_LOG(ERROR, "Invalid doc id(%d).", docId);
        return false;
    }

    // TODO: use MultiValueReader for performance
    auto status = _formatter->Decode(value.data(), value.size(), buffer, bufLen);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "decode section data fail. error [%s].", status.ToString().c_str());
        return false;
    }
    return true;
}

inline int32_t SectionAttributeReaderImpl::Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const
{
    assert(_sectionDataReader);
    assert(_formatter);
    bool ret = false;
    if (_sectionDataReader->IsIntegrated()) {
        ret = ReadAndDecodeSectionData(docId, buffer, bufLen, nullptr);
    } else {
        autil::mem_pool::Pool pool(128 * 1024);
        ret = ReadAndDecodeSectionData(docId, buffer, bufLen, &pool);
    }
    return ret ? 0 : -1;
}

} // namespace indexlib::index
