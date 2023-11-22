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

#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/section_attribute/InDocMultiSectionMeta.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"
#include "indexlib/index/normal/attribute/accessor/section_data_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

/**
 * section attribute reader implement
 */
class LegacySectionAttributeReaderImpl : public SectionAttributeReaderImpl
{
public:
    LegacySectionAttributeReaderImpl();
    virtual ~LegacySectionAttributeReaderImpl();

public:
    void Open(const config::PackageIndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData);

    // virtual for test
    SectionAttributeReader* Clone() const override;
    InDocSectionMetaPtr GetSection(docid_t docId) const override;
    int32_t Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const override;

private:
    bool ReadAndDecodeSectionData(docid_t docId, uint8_t* buffer, uint32_t bufLen, autil::mem_pool::Pool* pool) const;

private:
    legacy::SectionDataReaderPtr mSectionDataReader;
    common::SectionAttributeFormatterPtr mFormatter;

private:
    friend class LegacySectionAttributeReaderImplTest;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<LegacySectionAttributeReaderImpl> LegacySectionAttributeReaderImplPtr;

//////////////////////////////////////////////////////////////
inline std::shared_ptr<InDocSectionMeta> LegacySectionAttributeReaderImpl::GetSection(docid_t docId) const
{
    assert(_indexConfig);
    auto inDocSectionMeta = std::make_shared<InDocMultiSectionMeta>(_indexConfig);
    if (Read(docId, inDocSectionMeta->GetDataBuffer(), MAX_SECTION_BUFFER_LEN) < 0) {
        return nullptr;
    }
    inDocSectionMeta->UnpackInDocBuffer(HasFieldId(), HasSectionWeight());
    return inDocSectionMeta;
}

inline bool LegacySectionAttributeReaderImpl::ReadAndDecodeSectionData(docid_t docId, uint8_t* buffer, uint32_t bufLen,
                                                                       autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!mSectionDataReader->Read(docId, value, pool)) {
        IE_LOG(ERROR, "Invalid doc id(%d).", docId);
        return false;
    }

    // TODO: use MultiValueReader for performance
    mFormatter->Decode(value.data(), value.size(), buffer, bufLen);
    return true;
}

inline int32_t LegacySectionAttributeReaderImpl::Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const
{
    assert(mSectionDataReader);
    assert(mFormatter);
    bool ret = false;
    if (mSectionDataReader->IsIntegrated()) {
        ret = ReadAndDecodeSectionData(docId, buffer, bufLen, nullptr);
    } else {
        autil::mem_pool::Pool pool(128 * 1024);
        ret = ReadAndDecodeSectionData(docId, buffer, bufLen, &pool);
    }
    return ret ? 0 : -1;
}
}} // namespace indexlib::index
