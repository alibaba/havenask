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
#ifndef __INDEXLIB_INDEXPARTITIONSCHEMA_H
#define __INDEXLIB_INDEXPARTITIONSCHEMA_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/adaptive_dictionary_schema.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/region_schema.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/updateable_schema_standards.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, TruncateOptionConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchemaImpl);
DECLARE_REFERENCE_CLASS(config, SourceSchema);
DECLARE_REFERENCE_CLASS(config, FileCompressSchema);
DECLARE_REFERENCE_CLASS(test, SchemaMaker);

namespace indexlib { namespace config {

class IndexPartitionSchema;
DEFINE_SHARED_PTR(IndexPartitionSchema);

class IndexPartitionSchema : public autil::legacy::Jsonizable
{
public:
    explicit IndexPartitionSchema(const std::string& schemaName = "noname");
    ~IndexPartitionSchema() {}

public:
    static std::string GetSchemaFileName(schemavid_t schemaId);
    static IndexPartitionSchemaPtr LoadFromJsonStr(const std::string& jsonStr, bool loadFromIndex);
    static IndexPartitionSchemaPtr Load(const std::string& physicalPath, bool loadFromIndex = false);
    static void Store(const IndexPartitionSchemaPtr& schema, const std::string& physicalPath,
                      file_system::FenceContext* fenceContext);

public:
    std::shared_ptr<DictionaryConfig> AddDictionaryConfig(const std::string& dictName, const std::string& content);

    size_t GetFieldCount() const;
    fieldid_t GetFieldId(const std::string& fieldName) const;
    FieldConfigPtr GetFieldConfig(fieldid_t fieldId) const;
    FieldConfigPtr GetFieldConfig(const std::string& fieldName) const;
    const std::vector<std::shared_ptr<FieldConfig>>& GetFieldConfigs() const;
    FieldConfigPtr AddFieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue = false,
                                  bool isVirtual = false, bool isBinary = false);

    EnumFieldConfigPtr AddEnumFieldConfig(const std::string& fieldName, FieldType fieldType,
                                          std::vector<std::string>& validValues, bool multiValue = false);

    void AddRegionSchema(const RegionSchemaPtr& regionSchema);

    size_t GetRegionCount() const;

    const RegionSchemaPtr& GetRegionSchema(const std::string& regionName) const;

    const RegionSchemaPtr& GetRegionSchema(regionid_t regionId) const;

    regionid_t GetRegionId(const std::string& regionName) const;

    void SetSchemaName(const std::string& schemaName);

    const std::string& GetSchemaName() const;

    TableType GetTableType() const;
    const std::string& GetTableTypeV2() const;
    void SetTableType(const std::string& str);

    const autil::legacy::json::JsonMap& GetUserDefinedParam() const;

    autil::legacy::json::JsonMap& GetUserDefinedParam();

    void SetUserDefinedParam(const autil::legacy::json::JsonMap& jsonMap);

    bool GetValueFromUserDefinedParam(const std::string& key, std::string& value) const;

    void SetUserDefinedParam(const std::string& key, const std::string& value);

    const autil::legacy::json::JsonMap& GetGlobalRegionIndexPreference() const;

    autil::legacy::json::JsonMap& GetGlobalRegionIndexPreference();

    void SetGlobalRegionIndexPreference(const autil::legacy::json::JsonMap& jsonMap);

    void SetAdaptiveDictSchema(const AdaptiveDictionarySchemaPtr& adaptiveDictSchema);

    const AdaptiveDictionarySchemaPtr& GetAdaptiveDictSchema() const;

    const DictionarySchemaPtr& GetDictSchema() const;

    const FieldSchemaPtr& GetFieldSchema(regionid_t regionId) const;

    const IndexSchemaPtr& GetIndexSchema(regionid_t regionId) const;

    const AttributeSchemaPtr& GetAttributeSchema(regionid_t regionId) const;

    const AttributeSchemaPtr& GetVirtualAttributeSchema(regionid_t regionId) const;

    const SummarySchemaPtr& GetSummarySchema(regionid_t regionId) const;

    const SourceSchemaPtr& GetSourceSchema(regionid_t regionId) const;

    const std::shared_ptr<FileCompressSchema>& GetFileCompressSchema(regionid_t regionId) const;

    // for test
    void TEST_SetSourceSchema(const SourceSchemaPtr& sourceSchema);
    void TEST_SetFileCompressSchema(const std::shared_ptr<FileCompressSchema>& fileCompressSchema);

    const TruncateProfileSchemaPtr& GetTruncateProfileSchema(regionid_t regionId) const;

    const FieldSchemaPtr& GetFieldSchema() const;

    const IndexSchemaPtr& GetIndexSchema() const;

    const AttributeSchemaPtr& GetAttributeSchema() const;

    const AttributeSchemaPtr& GetVirtualAttributeSchema() const;

    const SummarySchemaPtr& GetSummarySchema() const;

    const SourceSchemaPtr& GetSourceSchema() const;

    const std::shared_ptr<FileCompressSchema>& GetFileCompressSchema() const;

    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const;

    const IndexPartitionSchemaPtr& GetSubIndexPartitionSchema() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void AssertEqual(const IndexPartitionSchema& other) const;

    // only literal clone (rewrite data not cloned)
    IndexPartitionSchema* Clone() const;

    // shallow clone
    IndexPartitionSchema* CloneWithDefaultRegion(regionid_t regionId) const;

    void CloneVirtualAttributes(const IndexPartitionSchema& other);

    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs, regionid_t id = DEFAULT_REGIONID);

    /**
     * #*this# is compatible with #other# if and only if:
     * (1) all fields and their fieldIds in #*this# must be the same in #other#
     * (2) all attributes field in #*this# must be in #other#
     * (3) all indexes in #*this# must also be in #other#
     * (4) all summary field in #*this# must also be in #other#
     */
    void AssertCompatible(const IndexPartitionSchema& other) const;

    void Check() const;

    /**
     * when
     *   mSummarySchema is NULL
     * or
     *   all summary fields are also in attribute schema
     * do not need store summary
     */
    bool NeedStoreSummary() const;

    bool IsUsefulField(const std::string& fieldName) const;

    void SetSubIndexPartitionSchema(const IndexPartitionSchemaPtr& schema);

    void SetEnableTTL(bool enableTTL, regionid_t id = DEFAULT_REGIONID,
                      const std::string& ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS);
    bool TTLEnabled(regionid_t id = DEFAULT_REGIONID) const;
    bool EnableTemperatureLayer(regionid_t id = DEFAULT_REGIONID) const;
    TemperatureLayerConfigPtr GetTemperatureLayerConfig(regionid_t id = DEFAULT_REGIONID);
    void SetTemperatureLayerConfig(const TemperatureLayerConfigPtr& config, regionid_t id = DEFAULT_REGIONID);
    void SetUpdateableSchemaStandards(const UpdateableSchemaStandards& standards, regionid_t id = DEFAULT_REGIONID);

    void SetEnableHashId(bool enableHashId, regionid_t id = DEFAULT_REGIONID,
                         const std::string& fieldName = DEFAULT_HASH_ID_FIELD_NAME);
    bool HashIdEnabled(regionid_t id = DEFAULT_REGIONID) const;
    const std::string& GetHashIdFieldName(regionid_t id = DEFAULT_REGIONID);

    void SetDefaultTTL(int64_t defaultTTL, regionid_t id = DEFAULT_REGIONID,
                       const std::string& ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS);

    int64_t GetDefaultTTL(regionid_t id = DEFAULT_REGIONID) const;

    const std::string& GetTTLFieldName(regionid_t id = DEFAULT_REGIONID) const;

    bool TTLFromDoc(regionid_t id = DEFAULT_REGIONID) const;

    void SetAutoUpdatePreference(bool autoUpdate);
    void SetInsertOrIgnore(bool insertOrIgnore);

    bool GetAutoUpdatePreference() const;

    bool SupportAutoUpdate() const;
    bool SupportInsertOrIgnore() const;
    bool SupportIndexUpdate() const { return false; }

    void LoadFieldSchema(const autil::legacy::Any& any);

    void SetDefaultRegionId(regionid_t regionId);

    regionid_t GetDefaultRegionId() const;

    void SetCustomizedTableConfig(const CustomizedTableConfigPtr& tableConfig);

    const CustomizedTableConfigPtr& GetCustomizedTableConfig() const;

    void SetCustomizedDocumentConfig(const CustomizedConfigVector& documentConfigs);

    const CustomizedConfigVector& GetCustomizedDocumentConfigs() const;

    bool HasModifyOperations() const;
    bool MarkOngoingModifyOperation(schema_opid_t opId);

    // schema_opid begin from 1
    const SchemaModifyOperationPtr& GetSchemaModifyOperation(schema_opid_t opId) const;
    size_t GetModifyOperationCount() const;

    /** return json format string **
     {
         "indexs" :
         [
             {
                 "name": "index1",
                 "type": "NUMBER",
                 "operation_id" : 1
             },
             ...
         ],
         "attributes" :
         [
             {
                 "name": "new_price",
                 "type": "int8",
                 "operation_id" : 2
             }
         ]
     }
     */
    std::string GetEffectiveIndexInfo(bool modifiedIndexOnly = false) const;
    void GetNotReadyModifyOperationIds(std::vector<schema_opid_t>& ids) const;
    void GetModifyOperationIds(std::vector<schema_opid_t>& ids) const;
    void GetDisableOperations(std::vector<schema_opid_t>& ret) const;

    /* TODO: remove
     * compitable usage for legacy schema file without format_version_id in IndexConfig */
    void SetLoadFromIndex(bool flag);
    bool IsLoadFromIndex();

public:
    // for test

    void SetSourceSchema(const SourceSchemaPtr& sourceSchema);
    void AddIndexConfig(const IndexConfigPtr& indexConfig, regionid_t id = DEFAULT_REGIONID);
    void AddAttributeConfig(const std::string& fieldName, regionid_t id = DEFAULT_REGIONID);
    void AddAttributeConfig(const AttributeConfigPtr& attrConfig, regionid_t id = DEFAULT_REGIONID);

    void AddPackAttributeConfig(const std::string& attrName, const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr, regionid_t id = DEFAULT_REGIONID,
                                const std::shared_ptr<FileCompressConfig>& FileCompressConfig = nullptr);

    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig,
                                   regionid_t id = DEFAULT_REGIONID);

    void AddSummaryConfig(const std::string& fieldName, summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID,
                          regionid_t id = DEFAULT_REGIONID);
    void SetTruncateProfileSchema(const TruncateProfileSchemaPtr& truncateProfileSchema,
                                  regionid_t id = DEFAULT_REGIONID);
    void LoadValueConfig(regionid_t id = DEFAULT_REGIONID);

    void ResetRegions();

    const FieldSchemaPtr& GetDefaultFieldSchema() const;

    schemavid_t GetSchemaVersionId() const;
    void SetSchemaVersionId(schemavid_t schemaId);

    const IndexPartitionSchemaImplPtr& GetImpl() const;

    void SetMaxModifyOperationCount(uint32_t count);
    uint32_t GetMaxModifyOperationCount() const;
    void EnableThrowAssertCompatibleException();

    IndexPartitionSchema* CreateSchemaForTargetModifyOperation(schema_opid_t opId) const;

    IndexPartitionSchema* CreateSchemaWithoutModifyOperations() const;
    bool HasUpdateableStandard() const;

    bool IsTablet() const;

public:
    static std::string TableType2Str(TableType tableType);
    static bool Str2TableType(const std::string& str, TableType& tableType);
    // for testlib
    const IndexPartitionSchemaImplPtr& GET_IMPL() const { return mImpl; }

private:
    IndexPartitionSchema* CreateSchemaWithMaxModifyOperationCount(uint32_t maxOperationCount) const;

private:
    IndexPartitionSchemaImplPtr mImpl;

private:
    friend class CheckCompabilityTest;
    friend class IndexPartitionSchemaTest;
    friend class SchemaConfiguratorTest;
    friend class test::SchemaMaker;
    friend class IndexConfigCreator;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IndexPartitionSchema);
}} // namespace indexlib::config

#endif //__INDEXLIB_INDEXPARTITIONSCHEMA_H
