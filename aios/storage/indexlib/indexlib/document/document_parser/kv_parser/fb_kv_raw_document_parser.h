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
#ifndef __INDEXLIB_FB_KV_RAW_DOCUMENT_PARSER_H
#define __INDEXLIB_FB_KV_RAW_DOCUMENT_PARSER_H

#include <memory>

#include "autil/MurmurHash.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/kv_document/kv_message_generated.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class FbKvRawDocumentParser : public RawDocumentParser
{
public:
    FbKvRawDocumentParser(const config::IndexPartitionSchemaPtr& schema);

public:
    virtual bool construct(const DocumentInitParamPtr& initParam);

public:
    bool parse(const std::string& docString, document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }
    bool parse(const RawDocumentParser::Message& msg, document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc) override;

protected:
    enum FieldFlag : uint8_t {
        FF_NONE = 0,
        FF_PKEY = 1,
        FF_SKEY = 1 << 1,
        FF_USER_TS = 1 << 2,
        FF_TTL = 1 << 3,
    };
    struct FieldProperty {
        config::FieldConfig* fieldConfig = nullptr;
        int16_t fieldIndexInPack = -1;
        FieldFlag flag = FF_NONE;
    };
    using FieldPropertyTable = std::unordered_map<uint64_t, FieldProperty>;

    struct RegionResource {
        std::unique_ptr<common::PackAttributeFormatter> packFormatter;
        std::unique_ptr<common::AttributeConvertor> dataConvertor;
        std::vector<autil::StringView> fieldsBuffer;
        FieldPropertyTable fieldPropertyTable;
        FieldType pKeyFieldType;
        FieldType sKeyFieldType;
        bool pKeyNumberHash = false;
        std::vector<std::unique_ptr<common::AttributeConvertor>> fieldConvertors;
    };

protected:
    uint64_t hash(const void* key, size_t len) const { return autil::MurmurHash::MurmurHash64A(key, len, 0); }
    bool initKeyHasher(regionid_t regionId);
    bool initFieldProperty(regionid_t regionId);
    bool initValueConvertor(regionid_t regionId);
    bool addProperty(const std::string& fieldName, regionid_t regionId, FieldFlag flag);
    bool validMessage(DocOperateType opType, bool hasPKey, bool hasSKey,
                      const std::vector<autil::StringView>& fields) const;
    template <typename T>
    std::pair<bool, T> getNumericFieldValue(const document::proto::kv::Field* field) const;
    std::pair<bool, uint64_t> getKeyHash(const document::proto::kv::Field* field, regionid_t regionId,
                                         FieldFlag flag) const;
    template <typename FbValue>
    autil::StringView convertNumericValue(const config::FieldConfig& fieldConfig, FbValue fbValue,
                                          autil::mem_pool::Pool* pool) const;
    autil::StringView getFieldValue(const document::proto::kv::Field* field, regionid_t regionId,
                                    const FieldProperty& property, autil::mem_pool::Pool* pool) const;

protected:
    config::IndexPartitionSchemaPtr _schema;
    bool _verify;
    DocumentInitParamPtr _initParam;
    std::vector<RegionResource> _regionResources;
    std::unique_ptr<common::AttributeConvertor> _simpleValueConvertor;
    std::string _userTimestampFieldName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FbKvRawDocumentParser);

}} // namespace indexlib::document

#endif
