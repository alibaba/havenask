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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, AttributeUpdateBitmap);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(document, AttributeDocument);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class AttributeModifier
{
public:
    typedef std::vector<AttributeUpdateBitmapPtr> PackAttrUpdateBitmapVec;

    AttributeModifier(const config::IndexPartitionSchemaPtr& schema, util::BuildResourceMetrics* buildResourceMetrics);
    virtual ~AttributeModifier();

public:
    virtual bool Update(docid_t docId, const document::AttributeDocumentPtr& attrDoc) = 0;
    virtual void UpdateAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc, attrid_t attrId)
    {
        assert(false);
    }
    virtual void UpdatePackAttribute(docid_t docId, const document::AttributeDocumentPtr& attrDoc,
                                     packattrid_t packAttrId)
    {
        assert(false);
    }

    virtual bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) = 0;

    PackAttrUpdateBitmapVec GetPackAttrUpdataBitmapVec() const { return mPackUpdateBitmapVec; }
    static void DumpPackAttributeUpdateInfo(const file_system::DirectoryPtr& attrDirectory,
                                            const config::IndexPartitionSchemaPtr& schema,
                                            const PackAttrUpdateBitmapVec& packUpdateBitmapVec);

private:
    void InitAttributeConvertorMap(const config::IndexPartitionSchemaPtr& schema);

protected:
    void InitPackAttributeUpdateBitmap(const index_base::PartitionDataPtr& partitionData);

    void DumpPackAttributeUpdateInfo(const file_system::DirectoryPtr& attrDirectory) const;

protected:
    typedef std::vector<common::AttributeConvertorPtr> FieldIdToConvertorMap;

    config::IndexPartitionSchemaPtr mSchema;
    FieldIdToConvertorMap mFieldId2ConvertorMap;
    PackAttrUpdateBitmapVec mPackUpdateBitmapVec;
    util::BuildResourceMetrics* mBuildResourceMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeModifier);
}} // namespace indexlib::index
