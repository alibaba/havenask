#ifndef __INDEXLIB_PACK_ATTRIBUTE_READER_H
#define __INDEXLIB_PACK_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/util/mem_buffer.h"

DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(index, PackAttributeIterator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(index);

class PackAttributeReader
{
private:
    typedef common::PackAttributeFormatter::PackAttributeFields PackAttributeFields;

public:
    PackAttributeReader(AttributeMetrics* attributeMetrics)
        : mAttrMetrics(attributeMetrics)
        , mBuildResourceMetricsNode(NULL)
    {}

    ~PackAttributeReader()
    {
        if (mAttrMetrics && mBuffer)
        {
            mAttrMetrics->DecreasePackAttributeReaderBufferSizeValue(
                    mBuffer->GetBufferSize());
        }
        mBuffer.reset();
    }

public:
    bool Open(const config::PackAttributeConfigPtr& packAttrConfig,
              const index_base::PartitionDataPtr& partitionData);

    void InitBuildResourceMetricsNode(
            util::BuildResourceMetrics* buildResourceMetrics);

    common::AttributeReference* GetSubAttributeReference(const std::string& subAttrName) const;
    common::AttributeReference* GetSubAttributeReference(attrid_t subAttrId) const;

    template <typename T>
    common::AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(
        const std::string& subAttrName) const;

    template <typename T>
    common::AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(
        attrid_t subAttrId) const;

    const char* GetBaseAddress(docid_t docId) const __ALWAYS_INLINE;

    AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool *pool,
            const std::string& subAttrName) const;

    PackAttributeIterator* CreateIterator(autil::mem_pool::Pool *pool);

    bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen);

    bool UpdatePackFields(docid_t docId, const PackAttributeFields& packFields,
                          bool hasHashKeyInFields);

    config::PackAttributeConfigPtr GetConfig() const {
        return mPackAttrConfig;
    }

public:
    // for tools
    bool Read(docid_t docId, const std::string& attrName, std::string& value,
              autil::mem_pool::Pool* pool = NULL) const;

private:
    void IncreaseAccessCounter(const common::AttributeReference* attrRef) const
    {
        if (attrRef && mAttrMetrics)
        {
            mAttrMetrics->IncAccessCounter(attrRef->GetAttrName());
        }
    }
    template<typename T>
    AttributeIteratorBase* CreateIteratorTyped(autil::mem_pool::Pool *pool,
            const std::string& subAttrName, bool isMulti) const;

    void InitBuffer();

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB

    config::PackAttributeConfigPtr mPackAttrConfig;
    StringAttributeReaderPtr mDataReader;
    AttributeMetrics* mAttrMetrics;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    util::MemBufferPtr mBuffer;
    common::AttributeConvertorPtr mDataConvertor;
    util::SimplePool mSimplePool;


private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackAttributeReader);

template <typename T>
inline common::AttributeReferenceTyped<T>*
PackAttributeReader::GetSubAttributeReferenceTyped(
    const std::string& subAttrName) const
{
    if (!mPackAttrFormatter)
    {
        return NULL;
    }
    common::AttributeReferenceTyped<T>* attrRef =
        mPackAttrFormatter->GetAttributeReferenceTyped<T>(subAttrName);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

template <typename T>
inline common::AttributeReferenceTyped<T>*
PackAttributeReader::GetSubAttributeReferenceTyped(
    attrid_t subAttrId) const
{
    if (!mPackAttrFormatter)
    {
        return NULL;
    }
    common::AttributeReferenceTyped<T>* attrRef =
        mPackAttrFormatter->GetAttributeReferenceTyped<T>(subAttrId);
    IncreaseAccessCounter(attrRef);
    return attrRef;
}

inline const char* PackAttributeReader::GetBaseAddress(docid_t docId) const
{
    autil::MultiValueType<char> value;
    if (!mDataReader->Read(docId, value))
    {
        return NULL;
    }
    return value.data();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_ATTRIBUTE_READER_H
