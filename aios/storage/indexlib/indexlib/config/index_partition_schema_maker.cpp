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
#include "indexlib/config/index_partition_schema_maker.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/config/schema_configurator.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, IndexPartitionSchemaMaker);

IndexPartitionSchemaMaker::IndexPartitionSchemaMaker() {}

IndexPartitionSchemaMaker::~IndexPartitionSchemaMaker() {}

void IndexPartitionSchemaMaker::MakeSchema(IndexPartitionSchemaPtr& schema, const string& fieldNames,
                                           const string& indexNames, const string& attributeNames,
                                           const string& summaryNames, const string& truncateProfileStr)
{
    if (fieldNames.size() > 0) {
        MakeFieldConfigSchema(schema, SpliteToStringVector(fieldNames));
    }
    if (indexNames.size() > 0) {
        MakeIndexConfigSchema(schema, SpliteToStringVector(indexNames));
    }

    if (attributeNames.size() > 0) {
        MakeAttributeConfigSchema(schema, SpliteToStringVector(attributeNames));
    }
    if (summaryNames.size() > 0) {
        MakeSummaryConfigSchema(schema, SpliteToStringVector(summaryNames));
    }
    RegionSchemaPtr regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
    if (regionSchema) {
        regionSchema.get()->SetNeedStoreSummary();
    }

    if (truncateProfileStr.size() > 0) {
        MakeTruncateProfiles(schema, truncateProfileStr);
    }
}

IndexPartitionSchemaMaker::StringVector IndexPartitionSchemaMaker::SpliteToStringVector(const string& names)
{
    StringVector sv;
    autil::StringTokenizer st(names, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        sv.push_back(*it);
    }
    return sv;
}

void IndexPartitionSchemaMaker::MakeFieldConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames)
{
    // make fieldSchema
    size_t i;
    for (i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":",
                                  autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() == 2 || st.getNumTokens() == 3 || st.getNumTokens() == 4 || st.getNumTokens() == 5 ||
               st.getNumTokens() == 6);
        // fieldName : fieldType : isMultiValue : updatableMultiValue : compressType : fixedMultiValueLength
        bool isMultiValue = false;
        if (st.getNumTokens() >= 3) {
            isMultiValue = (st[2] == "true");
        }
        FieldType fieldType = FieldConfig::StrToFieldType(st[1]);
        schema->AddFieldConfig(st[0], fieldType, isMultiValue);
        FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(st[0]);
        if (st.getNumTokens() >= 4) {
            if (st[3] == "true") {
                fieldConfig->SetUpdatableMultiValue(true);
            }
        }

        if (st.getNumTokens() >= 5) {
            fieldConfig->SetCompressType(st[4]);
        }

        if (st.getNumTokens() >= 6) {
            int32_t fixedMultiValueLength = -1;
            StringUtil::strToInt32(st[5].c_str(), fixedMultiValueLength);
            fieldConfig->SetFixedMultiValueCount(fixedMultiValueLength);
        }

        if (fieldType == ft_text) {
            fieldConfig->SetAnalyzerName("taobao_analyzer");
        }
    }
}

void IndexPartitionSchemaMaker::MakeIndexConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames)
{
    // make indexSchema
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        autil::StringTokenizer st(fieldNames[i], ":",
                                  autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        assert(st.getNumTokens() >= 3);
        string indexName = st[0];
        InvertedIndexType indexType = IndexConfig::StrToIndexType(st[1], schema->GetTableType());
        string fieldName = st[2];
        bool hasTruncate = false;
        if ((st.getNumTokens() == 4) && (st[3] == string("true"))) {
            hasTruncate = true;
        }

        autil::StringTokenizer st2(fieldName, ",",
                                   autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

        if (st2.getNumTokens() == 1 && indexType != it_pack && indexType != it_expack && indexType != it_customized) {
            SingleFieldIndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
                indexName, indexType, st2[0], "", false, schema->GetFieldSchema());
            indexConfig->SetHasTruncateFlag(hasTruncate);
            schema->AddIndexConfig(indexConfig);

            if (indexType == it_primarykey64 || indexType == it_primarykey128) {
                indexConfig->SetPrimaryKeyAttributeFlag(true);
                indexConfig->SetOptionFlag(of_none);
            }

            if (indexType == it_string || indexType == it_number) {
                indexConfig->SetOptionFlag(of_none | of_term_frequency);
            }

            if (indexType == it_number) {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetInvertedIndexType(indexType);
            }
        } else {
            vector<int32_t> boostVec;
            boostVec.resize(st2.getTokenVector().size(), 0);
            PackageIndexConfigPtr indexConfig = IndexConfigCreator::CreatePackageIndexConfig(
                indexName, indexType, st2.getTokenVector(), boostVec, "", false, schema->GetFieldSchema());
            indexConfig->SetHasTruncateFlag(hasTruncate);
            schema->AddIndexConfig(indexConfig);
            if (indexType == it_number) {
                NumberIndexTypeTransformor::Transform(indexConfig, indexType);
                indexConfig->SetInvertedIndexType(indexType);
            }
        }
    }
}

void IndexPartitionSchemaMaker::MakeAttributeConfigSchema(IndexPartitionSchemaPtr& schema,
                                                          const StringVector& fieldNames)
{
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        StringTokenizer st(fieldNames[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        assert(st.getNumTokens() == 1);
        schema->AddAttributeConfig(st[0]);
    }
}

void IndexPartitionSchemaMaker::MakeSummaryConfigSchema(IndexPartitionSchemaPtr& schema, const StringVector& fieldNames)
{
    // make SummarySchema
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        schema->AddSummaryConfig(fieldNames[i]);
    }
}

// truncateName0:sortDescription0#truncateName1:sortDescription1...
void IndexPartitionSchemaMaker::MakeTruncateProfiles(IndexPartitionSchemaPtr& schema, const string& truncateProfileStr)
{
    vector<vector<string>> truncateProfileVec;
    StringUtil::fromString(truncateProfileStr, truncateProfileVec, ":", "#");
    TruncateProfileSchemaPtr truncateProfileSchema(new TruncateProfileSchema());
    for (size_t i = 0; i < truncateProfileVec.size(); ++i) {
        string profileName = truncateProfileVec[i][0];
        string sortDesp = (truncateProfileVec[i].size() > 1) ? truncateProfileVec[i][1] : "";
        TruncateProfileConfigPtr truncateProfile(new TruncateProfileConfig(profileName, sortDesp));
        truncateProfileSchema->AddTruncateProfileConfig(truncateProfile);
    }
    schema->SetTruncateProfileSchema(truncateProfileSchema);
}
}} // namespace indexlib::config
