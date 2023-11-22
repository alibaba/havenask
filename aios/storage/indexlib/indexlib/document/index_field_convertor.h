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
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);
DECLARE_REFERENCE_CLASS(document, Section);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(common, RangeFieldEncoder);
DECLARE_REFERENCE_CLASS(common, DateFieldEncoder);
DECLARE_REFERENCE_CLASS(common, CustomizedIndexFieldEncoder);
DECLARE_REFERENCE_CLASS(index, SpatialFieldEncoder);

namespace indexlib { namespace document {

class IndexFieldConvertor
{
public:
    class TokenizeSection
    {
    public:
        typedef std::vector<std::string> TokenVector;
        typedef std::vector<std::string>::iterator Iterator;

    public:
        TokenizeSection() {}
        ~TokenizeSection() {}

    public:
        Iterator Begin() { return mTokens.begin(); }
        Iterator End() { return mTokens.end(); }

        void SetTokenVector(const TokenVector& tokenVec) { mTokens = tokenVec; }
        size_t GetTokenCount() const { return mTokens.size(); }

    private:
        TokenVector mTokens;
    };
    DEFINE_SHARED_PTR(TokenizeSection);

public:
    IndexFieldConvertor(const config::IndexPartitionSchemaPtr& schema, regionid_t regionId = DEFAULT_REGIONID);
    ~IndexFieldConvertor();

public:
    void convertIndexField(const document::IndexDocumentPtr& indexDoc, const config::FieldConfigPtr& fieldConfig,
                           const std::string& fieldStr, const std::string& fieldSep, autil::mem_pool::Pool* pool);

    static TokenizeSectionPtr ParseSection(const std::string& sectionStr, const std::string& sep);

private:
    void init();
    void initFieldTokenHasherTypeVector();

    bool addSection(document::IndexTokenizeField* field, const std::string& fieldValue, const std::string& fieldSep,
                    autil::mem_pool::Pool* pool, fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos);

    bool addToken(document::Section* indexSection, const std::string& token, autil::mem_pool::Pool* pool,
                  fieldid_t fieldId, pos_t& lastTokenPos, pos_t& curPos);

    void addSpatialSection(fieldid_t fieldId, const std::string& fieldValue, document::IndexTokenizeField* field);

    void addDateSection(fieldid_t fieldId, const std::string& fieldValue, document::IndexTokenizeField* field);

    void addFloatSection(fieldid_t fieldId, const std::string& fieldValue, const std::string& fieldSep,
                         document::IndexTokenizeField* field);

    bool addHashToken(document::Section* indexSection, dictkey_t hashKey, pos_t& lastTokenPos, pos_t& curPos);

    void addSection(const std::vector<dictkey_t>& dictKeys, document::IndexTokenizeField* field);

    void addRangeSection(fieldid_t fieldId, const std::string& fieldValue, document::IndexTokenizeField* field);

private:
    std::vector<index::TokenHasher> mFieldIdToTokenHasher;
    config::IndexPartitionSchemaPtr mSchema;
    std::shared_ptr<indexlib::index::SpatialFieldEncoder> mSpatialFieldEncoder;
    std::shared_ptr<indexlib::common::DateFieldEncoder> mDateFieldEncoder;
    std::shared_ptr<indexlib::common::RangeFieldEncoder> mRangeFieldEncoder;
    common::CustomizedIndexFieldEncoderPtr mCustomizedIndexFieldEncoder;
    regionid_t mRegionId;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IndexFieldConvertor);
}} // namespace indexlib::document
