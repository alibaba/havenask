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
#ifndef __INDEXLIB_TEST_SCHEMA_MAKER_H
#define __INDEXLIB_TEST_SCHEMA_MAKER_H

#include <memory>

#include "autil/legacy/any.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::util {
class TabletSchemaMaker;
}

namespace indexlib { namespace test {

class SchemaMaker
{
public:
    typedef std::vector<std::string> StringVector;

    SchemaMaker() {}
    ~SchemaMaker() {}

public:
    static config::IndexPartitionSchemaPtr MakeSchema(const std::string& fieldNames, const std::string& indexNames,
                                                      const std::string& attributeNames,
                                                      const std::string& summaryNames,
                                                      const std::string& truncateProfileStr = "",
                                                      const std::string& sourceFields = "");
    static config::IndexPartitionSchemaPtr MakeKKVSchema(const std::string& fieldNames, const std::string& pkeyName,
                                                         const std::string& skeyName, const std::string& valueNames,
                                                         int64_t ttl = INVALID_TTL,
                                                         const std::string& valueFormat = "");
    static config::IndexPartitionSchemaPtr MakeKVSchema(const std::string& fieldNames, const std::string& keyName,
                                                        const std::string& valueNames, int64_t ttl = INVALID_TTL,
                                                        const std::string& valueFormat = "", bool useNumberHash = true);

    static void SetAdaptiveHighFrequenceDictionary(const std::string& indexName, const std::string& adaptiveRuleStr,
                                                   index::HighFrequencyTermPostingType type,
                                                   const config::IndexPartitionSchemaPtr& schema);

    // fieldInfos: fieldName:[default_null_string]:[user_define_null_value];......
    // Note: not recursive
    static void EnableNullFields(const config::IndexPartitionSchemaPtr& schema, const std::string& fieldInfos);

    // fileCompressInfoStr : compressorName:compresorType:[HOT#compressorName|WARM#compressorName];....
    // indexCompressorStr : indexName:compressorName;indexName1:compressorName1
    // attributeCompressorStr : attrName:compressorName;attrName1:compressorName1
    /* example:
    string fileCompressInfoStr = "cold_compressor:zstd;"
                                 "warm_compressor:lz4;"
                                 "temperature_compressor:null:WARM#warm_compressor|COLD#cold_compressor";
    string indexCompressStr = "index1:temperature_compressor;index2:warm_compressor";
    string attrCompressStr = "string1:temperature_compressor;price:temperature_compressor";
    string summaryCompressStr = "0:temperature_compressor;1:temperature_compressor";
    string sourceCompressStr = "0:temperature_compressor;1:temperature_compressor";
    */
    static config::IndexPartitionSchemaPtr
    EnableFileCompressSchema(const config::IndexPartitionSchemaPtr& schema, const std::string& fileCompressInfoStr,
                             const std::string& indexCompressorStr = "", const std::string& attrCompressorStr = "",
                             const std::string& summaryCompressStr = "", const std::string& sourceCompressStr = "");

    // temperatureConfigPtr: HOT#[conditionStr&conditionStr]|WARM#[conditionStr;...]
    // conditionStr: key1=value1;key2=value2
    /* example:
      string temperatureLayerStr = "WARM#type=Range;field_name=price;value=[10,99];value_type=UINT64|"
                                   "COLD#type=Range;field_name=price;value=[100,1000];value_type=UINT64";
     */
    static void EnableTemperatureLayer(const config::IndexPartitionSchemaPtr& schema,
                                       const std::string& temperatureConfigStr = "",
                                       const std::string& defaultTemperatureLayer = "HOT");

public:
    // for testlib
    static void MakeFieldConfigForSchema(config::IndexPartitionSchemaPtr& schema, const std::string& fieldNames)
    {
        MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    }

    static std::vector<config::IndexConfigPtr> MakeIndexConfigs(const std::string& indexNames,
                                                                const config::IndexPartitionSchemaPtr& schema);

    static config::FieldSchemaPtr MakeFieldSchema(const std::string& fieldNames);

    static void MakeAttributeConfigForSchema(config::IndexPartitionSchemaPtr& schema, const std::string& fieldNames,
                                             regionid_t regionId = DEFAULT_REGIONID)
    {
        MakeAttributeConfigSchema(schema, SplitToStringVector(fieldNames), regionId);
    }

    static void MakeKKVIndex(config::IndexPartitionSchemaPtr& schema, const std::string& pkeyName,
                             const std::string& skeyName, regionid_t regionId = DEFAULT_REGIONID,
                             int64_t ttl = INVALID_TTL, const std::string& valueFormat = "");

    static void MakeKVIndex(config::IndexPartitionSchemaPtr& schema, const std::string& keyName,
                            regionid_t regionId = DEFAULT_REGIONID, int64_t ttl = INVALID_TTL,
                            const std::string& valueFormat = "", bool useNumberHash = true);

public:
    static StringVector SplitToStringVector(const std::string& names);

private:
    static void MakeFieldConfigSchema(config::IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeIndexConfigSchema(config::IndexPartitionSchemaPtr& schema, const std::string& indexNames);

    static void MakeAttributeConfigSchema(config::IndexPartitionSchemaPtr& schema, const StringVector& fieldNames,
                                          regionid_t regionId = DEFAULT_REGIONID);

    static void MakeSummaryConfigSchema(config::IndexPartitionSchemaPtr& schema, const StringVector& fieldNames);

    static void MakeTruncateProfiles(config::IndexPartitionSchemaPtr& schema, const std::string& truncateProfileStr);

    static void MakeSourceSchema(config::IndexPartitionSchemaPtr& schema, const std::string& sourceFieldsStr);

private:
    friend class ModifySchemaMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaMaker);
}} // namespace indexlib::test

#endif //__INDEXLIB_TEST_SCHEMA_MAKER_H
