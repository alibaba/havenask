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
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/attribute/MultiValueAttributeReader.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReferenceTyped.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/pack_attribute/PackAttributeIterator.h"

namespace indexlibv2::index {
class PackAttributeConfig;
class PackAttributeMetrics;

class PackAttributeReader : public IIndexReader
{
public:
    PackAttributeReader(const std::shared_ptr<PackAttributeMetrics>& packAttributeMetrics);
    ~PackAttributeReader();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;

public:
    AttributeReference* GetSubAttributeReference(const std::string& subAttrName) const;
    AttributeReference* GetSubAttributeReference(attrid_t subAttrId) const;
    template <typename T>
    AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(const std::string& subAttrName) const;
    template <typename T>
    AttributeReferenceTyped<T>* GetSubAttributeReferenceTyped(attrid_t subAttrId) const;
    const char* GetBaseAddress(docid_t docId, autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool* pool, const std::string& subAttrName) const;
    PackAttributeIterator* CreateIterator(autil::mem_pool::Pool* pool);
    std::shared_ptr<PackAttributeConfig> GetPackAttributeConfig() const { return _packAttributeConfig; }

public:
    bool Read(docid_t docId, const std::string& attrName, std::string& value, autil::mem_pool::Pool* pool = NULL) const;

private:
    void IncreaseAccessCounter(const AttributeReference* attributeRef) const;
    template <typename T>
    AttributeIteratorBase* CreateIteratorTyped(autil::mem_pool::Pool* pool, const std::string& subAttrName,
                                               bool isMulti) const;
    const AttributeFieldPrinter* GetFieldPrinter(const std::string& subAttrName) const;
    void InitBuffer();

private:
    // static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB
    // util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    // util::MemBufferPtr mBuffer;
    // common::AttributeConvertorPtr mDataConvertor;
    // util::SimplePool mSimplePool;
    std::shared_ptr<PackAttributeConfig> _packAttributeConfig;
    std::unique_ptr<PackAttributeFormatter> _packAttributeFormatter;
    std::unique_ptr<MultiValueAttributeReader<char>> _dataReader;
    std::map<std::string, AttributeFieldPrinter> _fieldPrinters;
    std::shared_ptr<PackAttributeMetrics> _packAttributeMetrics;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
inline AttributeReferenceTyped<T>*
PackAttributeReader::GetSubAttributeReferenceTyped(const std::string& subAttrName) const
{
    if (!_packAttributeFormatter) {
        return NULL;
    }
    AttributeReferenceTyped<T>* attributeRef = _packAttributeFormatter->GetAttributeReferenceTyped<T>(subAttrName);
    IncreaseAccessCounter(attributeRef);
    return attributeRef;
}

template <typename T>
inline AttributeReferenceTyped<T>* PackAttributeReader::GetSubAttributeReferenceTyped(attrid_t subAttrId) const
{
    if (!_packAttributeFormatter) {
        return NULL;
    }
    AttributeReferenceTyped<T>* attributeRef = _packAttributeFormatter->GetAttributeReferenceTyped<T>(subAttrId);
    IncreaseAccessCounter(attributeRef);
    return attributeRef;
}

inline const char* PackAttributeReader::GetBaseAddress(docid_t docId, autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!_dataReader->Read(docId, value, pool)) {
        return NULL;
    }
    return value.data();
}

} // namespace indexlibv2::index
