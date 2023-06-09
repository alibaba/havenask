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
#ifndef __INDEXLIB_PACK_ATTRIBUTE_READER_H
#define __INDEXLIB_PACK_ATTRIBUTE_READER_H

#include <memory>

#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(index, PackAttributeIterator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

namespace indexlib { namespace index {

class PackAttributeReader
{
private:
    typedef common::PackAttributeFormatter::PackAttributeFields PackAttributeFields;

public:
    PackAttributeReader(AttributeMetrics* attributeMetrics)
        : mAttrMetrics(attributeMetrics)
        , mBuildResourceMetricsNode(NULL)
        , mUpdatble(false)
    {
    }

    ~PackAttributeReader()
    {
        if (mAttrMetrics && mBuffer) {
            mAttrMetrics->DecreasePackAttributeReaderBufferSizeValue(mBuffer->GetBufferSize());
        }
        mBuffer.reset();
    }

public:
    bool Open(const config::PackAttributeConfigPtr& packAttrConfig, const index_base::PartitionDataPtr& partitionData,
              const PackAttributeReader* hintReader = nullptr);

    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics);

    common::AttributeReference* GetSubAttributeReference(const std::string& subAttrName) const;
    common::AttributeReference* GetSubAttributeReference(attrid_t subAttrId) const;

    template <typename T>
    common::AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(const std::string& subAttrName) const;

    template <typename T>
    common::AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(attrid_t subAttrId) const;

    const char* GetBaseAddress(docid_t docId, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool* pool, const std::string& subAttrName) const;

    PackAttributeIterator* CreateIterator(autil::mem_pool::Pool* pool);

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen);

    bool UpdatePackFields(docid_t docId, const PackAttributeFields& packFields, bool hasHashKeyInFields);

    config::PackAttributeConfigPtr GetConfig() const { return mPackAttrConfig; }

public:
    // for tools
    bool Read(docid_t docId, const std::string& attrName, std::string& value, autil::mem_pool::Pool* pool = NULL) const;

private:
    void IncreaseAccessCounter(const common::AttributeReference* attrRef) const
    {
        if (attrRef && mAttrMetrics) {
            mAttrMetrics->IncAccessCounter(attrRef->GetAttrName());
        }
    }
    template <typename T>
    AttributeIteratorBase* CreateIteratorTyped(autil::mem_pool::Pool* pool, const std::string& subAttrName,
                                               bool isMulti) const;

    void InitBuffer();

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB
    typedef VarNumAttributeReader<char> StringAttributeReader;
    DEFINE_SHARED_PTR(StringAttributeReader);

    config::PackAttributeConfigPtr mPackAttrConfig;
    StringAttributeReaderPtr mDataReader;
    AttributeMetrics* mAttrMetrics;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    util::MemBufferPtr mBuffer;
    common::AttributeConvertorPtr mDataConvertor;
    util::SimplePool mSimplePool;
    bool mUpdatble;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeReader);

template <typename T>
inline common::AttributeReferenceTyped<T>*
PackAttributeReader::GetSubAttributeReferenceTyped(const std::string& subAttrName) const
{
    if (!mPackAttrFormatter) {
        return NULL;
    }
    common::AttributeReferenceTyped<T>* attrRef = mPackAttrFormatter->GetAttributeReferenceTyped<T>(subAttrName);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

template <typename T>
inline common::AttributeReferenceTyped<T>* PackAttributeReader::GetSubAttributeReferenceTyped(attrid_t subAttrId) const
{
    if (!mPackAttrFormatter) {
        return NULL;
    }
    common::AttributeReferenceTyped<T>* attrRef = mPackAttrFormatter->GetAttributeReferenceTyped<T>(subAttrId);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

inline const char* PackAttributeReader::GetBaseAddress(docid_t docId, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!mDataReader->Read(docId, value, pool)) {
        return NULL;
    }
    return value.data();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PACK_ATTRIBUTE_READER_H
