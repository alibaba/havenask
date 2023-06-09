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
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, MultiRegionPackAttributeAppender);

MultiRegionPackAttributeAppender::MultiRegionPackAttributeAppender() {}

MultiRegionPackAttributeAppender::~MultiRegionPackAttributeAppender() {}

bool MultiRegionPackAttributeAppender::Init(const IndexPartitionSchemaPtr& schema)
{
    if (!schema) {
        return false;
    }
    for (regionid_t id = 0; id < (regionid_t)schema->GetRegionCount(); id++) {
        PackAttributeAppenderPtr appender(new PackAttributeAppender);
        if (!appender->Init(schema, id)) {
            return false;
        }
        mAppenders.push_back(appender);
    }
    return true;
}

bool MultiRegionPackAttributeAppender::AppendPackAttribute(const AttributeDocumentPtr& attrDocument, Pool* pool,
                                                           regionid_t regionId)
{
    if (regionId >= (regionid_t)mAppenders.size()) {
        return false;
    }
    return mAppenders[regionId]->AppendPackAttribute(attrDocument, pool);
}

bool MultiRegionPackAttributeAppender::EncodePatchValues(const AttributeDocumentPtr& attrDocument, Pool* pool,
                                                         regionid_t regionId)
{
    if (regionId >= (regionid_t)mAppenders.size()) {
        return false;
    }
    return mAppenders[regionId]->EncodePatchValues(attrDocument, pool);
}

bool MultiRegionPackAttributeAppender::DecodePatchValues(uint8_t* buffer, size_t bufferLen, regionid_t regionId,
                                                         packattrid_t packId,
                                                         PackAttributeFormatter::PackAttributeFields& patchFields)
{
    if (regionId >= (regionid_t)mAppenders.size()) {
        return false;
    }
    return mAppenders[regionId]->DecodePatchValues(buffer, bufferLen, packId, patchFields);
}

autil::StringView MultiRegionPackAttributeAppender::MergeAndFormatUpdateFields(
    const char* baseAddr, const PackAttributeFormatter::PackAttributeFields& packAttrFields,
    bool hasHashKeyInAttrFields, util::MemBuffer& buffer, regionid_t regionId, packattrid_t packId)
{
    if (regionId >= (regionid_t)mAppenders.size()) {
        return autil::StringView::empty_instance();
    }
    return mAppenders[regionId]->MergeAndFormatUpdateFields(baseAddr, packAttrFields, hasHashKeyInAttrFields, buffer,
                                                            packId);
}

}} // namespace indexlib::document
