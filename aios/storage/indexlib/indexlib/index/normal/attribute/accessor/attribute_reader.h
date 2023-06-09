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
#ifndef __INDEXLIB_ATTRIBUTE_READER_H
#define __INDEXLIB_ATTRIBUTE_READER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, JoinDocidCacheReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeIteratorBase);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, TemperatureDocInfo);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {
enum PatchApplyStrategy : unsigned int;

#define DECLARE_ATTRIBUTE_READER_IDENTIFIER(id)                                                                        \
    static std::string Identifier() { return std::string("attribute.reader." #id); }                                   \
    virtual std::string GetIdentifier() const override { return Identifier(); }

class AttributeReader
{
public:
    AttributeReader(AttributeMetrics* metrics = NULL) : mAttributeMetrics(metrics), mEnableAccessCountors(false) {}
    virtual ~AttributeReader() {}

public:
    virtual bool Open(const config::AttributeConfigPtr& attrConfig, const index_base::PartitionDataPtr& partitionData,
                      PatchApplyStrategy patchApplyStrategy, const AttributeReader* hintReader = nullptr) = 0;

    virtual bool Read(docid_t docId, std::string& attrValue, autil::mem_pool::Pool* pool = NULL) const = 0;

    /**
     * Get attribute type
     */
    virtual AttrType GetType() const = 0;

    /**
     * Get multi value flag
     */
    virtual bool IsMultiValue() const = 0;

    virtual std::string GetIdentifier() const = 0;

    virtual AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool* pool) const = 0;

    virtual bool GetSortedDocIdRange(const table::DimensionDescription::Range& range, const DocIdRange& rangeLimit,
                                     DocIdRange& resultRange) const = 0;

    virtual bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull = false) { return false; }
    virtual bool Updatable() const = 0;

    virtual std::string GetAttributeName() const
    {
        assert(false);
        return "";
    }

    virtual JoinDocidCacheReader*
    CreateJoinDocidCacheReader(const PrimaryKeyIndexReaderPtr& pkIndexReader,
                               const DeletionMapReaderPtr& deletionMapReader,
                               const AttributeReaderPtr& attributeReader = AttributeReaderPtr())
    {
        assert(false);
        return NULL;
    }

    virtual size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                                    const config::AttributeConfigPtr& attrConfig,
                                    const index_base::Version& lastLoadVersion)
    {
        assert(false);
        return 0;
    }
    virtual AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const
    {
        assert(false);
        return AttributeSegmentReaderPtr();
    };

    virtual void EnableAccessCountors() { assert(false); }
    virtual void EnableGlobalReadContext() { assert(false); }

protected:
    // some attribute base directory may be not attribute directory
    // e.g. pk attribute, section
    virtual file_system::DirectoryPtr GetAttributeDirectory(const index_base::SegmentData& segmentData,
                                                            const config::AttributeConfigPtr& attrConfig) const
    {
        return file_system::DirectoryPtr();
    }

protected:
    AttributeMetrics* mAttributeMetrics;
    index_base::TemperatureDocInfoPtr mTemperatureDocInfo;
    bool mEnableAccessCountors;
};

typedef std::map<std::string, size_t> AttributeName2PosMap;
typedef std::vector<AttributeReaderPtr> AttributeReaderVector;

DEFINE_SHARED_PTR(PrimaryKeyIndexReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_READER_H
