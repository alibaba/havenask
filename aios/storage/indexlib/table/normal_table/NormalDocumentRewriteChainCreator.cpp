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
#include "indexlib/table/normal_table/NormalDocumentRewriteChainCreator.h"

#include "indexlib/base/Types.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/document_rewriter/DocumentInfoToAttributeRewriter.h"
#include "indexlib/document/document_rewriter/DocumentRewriteChain.h"
#include "indexlib/document/document_rewriter/TTLSetter.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalDocumentRewriteChainCreator);

std::pair<Status, std::shared_ptr<document::DocumentRewriteChain>>
NormalDocumentRewriteChainCreator::Create(const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto rewriteChain = std::make_shared<document::DocumentRewriteChain>();
    RETURN2_IF_STATUS_ERROR(AppendDocInfoRewriterIfNeed(schema, rewriteChain.get()), nullptr,
                            "document rewrite for append doc info failed");
    RETURN2_IF_STATUS_ERROR(AppendTTLSetterIfNeed(schema, rewriteChain.get()), nullptr, "append ttl setter failed");
    return {Status::OK(), rewriteChain};
}

Status
NormalDocumentRewriteChainCreator::AppendDocInfoRewriterIfNeed(const std::shared_ptr<config::ITabletSchema>& schema,
                                                               document::DocumentRewriteChain* rewriteChain)
{
    if (indexlib::table::TABLE_TYPE_NORMAL == schema->GetTableType()) {
        auto virtualTimestampAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(schema->GetIndexConfig(
            VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, document::DocumentInfoToAttributeRewriter::VIRTUAL_TIMESTAMP_FIELD_NAME));
        assert(virtualTimestampAttrConfig);
        auto timestampLegacyConf = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(
            virtualTimestampAttrConfig->GetAttributeConfig());
        assert(timestampLegacyConf);

        auto virtualHashIdAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(schema->GetIndexConfig(
            VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR, document::DocumentInfoToAttributeRewriter::VIRTUAL_HASH_ID_FIELD_NAME));
        assert(virtualHashIdAttrConfig);
        auto hashIdLegacyConf = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(
            virtualHashIdAttrConfig->GetAttributeConfig());
        assert(hashIdLegacyConf);

        auto virtualConcurrentIdxAttrConfig = std::dynamic_pointer_cast<VirtualAttributeConfig>(
            schema->GetIndexConfig(VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR,
                                   document::DocumentInfoToAttributeRewriter::VIRTUAL_CONCURRENT_IDX_FIELD_NAME));
        assert(virtualConcurrentIdxAttrConfig);
        auto concurrentIdxLegacyConf = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(
            virtualConcurrentIdxAttrConfig->GetAttributeConfig());
        assert(concurrentIdxLegacyConf);

        auto docInfoRewriter = std::make_shared<document::DocumentInfoToAttributeRewriter>(
            timestampLegacyConf, hashIdLegacyConf, concurrentIdxLegacyConf);
        rewriteChain->AppendRewriter(docInfoRewriter);
    }
    return Status::OK();
}

Status NormalDocumentRewriteChainCreator::AppendTTLSetterIfNeed(const std::shared_ptr<config::ITabletSchema>& schema,
                                                                document::DocumentRewriteChain* rewriteChain)
{
    if (indexlib::table::TABLE_TYPE_NORMAL == schema->GetTableType()) {
        auto [status, enableTTL] = schema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
        if (status.IsOK()) {
            if (!enableTTL) {
                return Status::OK();
            }
            auto [statusTTLField, ttlFieldName] = schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
            auto [statusDefaultTTL, defaultTTL] = schema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            RETURN_IF_STATUS_ERROR(statusTTLField, "get ttl_filed_name failed");
            RETURN_IF_STATUS_ERROR(statusDefaultTTL, "get default_ttl failed");
            assert(!ttlFieldName.empty());
            auto attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(
                schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, ttlFieldName));
            uint32_t defaultTTL32 = std::min(defaultTTL, (int64_t)std::numeric_limits<uint32_t>::max());
            if (attrConfig) {
                auto ttlSetter = std::make_shared<document::TTLSetter>(attrConfig, defaultTTL32);
                rewriteChain->AppendRewriter(ttlSetter);
            } else {
                assert(false);
                return Status::InternalError("cannot get ttl field [%s] attribute config", ttlFieldName.c_str());
            }
        } else if (!status.IsNotFound()) {
            return status;
        }
    }
    return Status::OK();
}
} // namespace indexlibv2::table
