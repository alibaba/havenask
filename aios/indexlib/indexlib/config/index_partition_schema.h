#ifndef __INDEXLIB_INDEXPARTITIONSCHEMA_H
#define __INDEXLIB_INDEXPARTITIONSCHEMA_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/adaptive_dictionary_schema.h"
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/region_schema.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/schema_modify_operation.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, TruncateOptionConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchemaImpl);
DECLARE_REFERENCE_CLASS(testlib, SchemaMaker);

IE_NAMESPACE_BEGIN(config);

class IndexPartitionSchema;
DEFINE_SHARED_PTR(IndexPartitionSchema);

class IndexPartitionSchema : public autil::legacy::Jsonizable
{
public:
    IndexPartitionSchema(const std::string& schemaName = "noname");;
    ~IndexPartitionSchema() {}

public:
    DictionaryConfigPtr AddDictionaryConfig(const std::string& dictName, 
                                            const std::string& content);

    FieldConfigPtr AddFieldConfig(const std::string& fieldName, 
                                  FieldType fieldType, 
                                  bool multiValue = false,
                                  bool isVirtual = false,
                                  bool isBinary = false);
    
    EnumFieldConfigPtr AddEnumFieldConfig(const std::string& fieldName, 
            FieldType fieldType, std::vector<std::string>& validValues, 
            bool multiValue = false);

    void AddRegionSchema(const RegionSchemaPtr& regionSchema);

    size_t GetRegionCount() const;

    const RegionSchemaPtr& GetRegionSchema(const std::string& regionName) const;

    const RegionSchemaPtr& GetRegionSchema(regionid_t regionId) const;
    
    regionid_t GetRegionId(const std::string& regionName) const;
    
    void SetSchemaName(const std::string& schemaName);
    
    const std::string& GetSchemaName() const;

    TableType GetTableType() const;
    void SetTableType(const std::string &str);

    const autil::legacy::json::JsonMap &GetUserDefinedParam() const;
    
    autil::legacy::json::JsonMap &GetUserDefinedParam();
    
    void SetUserDefinedParam(const autil::legacy::json::JsonMap &jsonMap);
    
    bool GetValueFromUserDefinedParam(const std::string &key, std::string &value) const;

    const autil::legacy::json::JsonMap &GetGlobalRegionIndexPreference() const;
    
    autil::legacy::json::JsonMap &GetGlobalRegionIndexPreference();
    
    void SetGlobalRegionIndexPreference(const autil::legacy::json::JsonMap &jsonMap);

    void SetAdaptiveDictSchema(const AdaptiveDictionarySchemaPtr& adaptiveDictSchema);
    
    const AdaptiveDictionarySchemaPtr& GetAdaptiveDictSchema() const;
    
    const DictionarySchemaPtr& GetDictSchema() const;
    
    const FieldSchemaPtr& GetFieldSchema(regionid_t regionId) const;
    
    const IndexSchemaPtr& GetIndexSchema(regionid_t regionId) const;
    
    const AttributeSchemaPtr& GetAttributeSchema(regionid_t regionId) const;
    
    const AttributeSchemaPtr& GetVirtualAttributeSchema(regionid_t regionId) const;
    
    const SummarySchemaPtr& GetSummarySchema(regionid_t regionId) const;
    
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema(regionid_t regionId) const;
    
    const FieldSchemaPtr& GetFieldSchema() const;

    const IndexSchemaPtr& GetIndexSchema() const;

    const AttributeSchemaPtr& GetAttributeSchema() const;
    
    const AttributeSchemaPtr& GetVirtualAttributeSchema() const;
    
    const SummarySchemaPtr& GetSummarySchema() const;
    
    const TruncateProfileSchemaPtr& GetTruncateProfileSchema() const;
    
    const IndexPartitionSchemaPtr& GetSubIndexPartitionSchema() const;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    void AssertEqual(const IndexPartitionSchema& other) const;

    // only literal clone (rewrite data not cloned)
    IndexPartitionSchema* Clone() const;

    // shallow clone 
    IndexPartitionSchema* CloneWithDefaultRegion(regionid_t regionId) const;
    
    void CloneVirtualAttributes(const IndexPartitionSchema& other);
    
    bool AddVirtualAttributeConfigs(const AttributeConfigVector& virtualAttrConfigs,
                                    regionid_t id = DEFAULT_REGIONID);

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

    void SetEnableTTL(bool enableTTL,
                      regionid_t id = DEFAULT_REGIONID,
                      const std::string& ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS);
    
    bool TTLEnabled(regionid_t id = DEFAULT_REGIONID) const;
    
    void SetDefaultTTL(int64_t defaultTTL, regionid_t id = DEFAULT_REGIONID,
                       const std::string& ttlFieldName = DOC_TIME_TO_LIVE_IN_SECONDS);
    
    int64_t GetDefaultTTL(regionid_t id = DEFAULT_REGIONID) const;
    
    const std::string& GetTTLFieldName(regionid_t id = DEFAULT_REGIONID);
    
    void SetAutoUpdatePreference(bool autoUpdate);

    bool GetAutoUpdatePreference() const;

    bool SupportAutoUpdate() const;
    
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
    void GetNotReadyModifyOperationIds(std::vector<schema_opid_t> &ids) const;
    void GetModifyOperationIds(std::vector<schema_opid_t> &ids) const;

public:
    // for testlib
    void AddIndexConfig(const IndexConfigPtr& indexConfig, regionid_t id = DEFAULT_REGIONID);
    void AddAttributeConfig(const std::string& fieldName, regionid_t id = DEFAULT_REGIONID);
    
    void AddPackAttributeConfig(const std::string& attrName,
                                const std::vector<std::string>& subAttrNames,
                                const std::string& compressTypeStr,
                                regionid_t id = DEFAULT_REGIONID);

    void AddVirtualAttributeConfig(const config::AttributeConfigPtr& virtualAttrConfig,
                                   regionid_t id = DEFAULT_REGIONID);

    void AddSummaryConfig(const std::string& fieldName,
                          summarygroupid_t summaryGroupId = DEFAULT_SUMMARYGROUPID,
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
    
    IndexPartitionSchema* CreateSchemaForTargetModifyOperation(
            schema_opid_t opId) const;

    IndexPartitionSchema* CreateSchemaWithoutModifyOperations() const;
    
public:
    static std::string TableType2Str(TableType tableType);
    // for testlib
    const IndexPartitionSchemaImplPtr& GET_IMPL() const { return mImpl; }

private:
    IndexPartitionSchema* CreateSchemaWithMaxModifyOperationCount(
            uint32_t maxOperationCount) const;
private:
    IndexPartitionSchemaImplPtr mImpl;
    
private:
    friend class CheckCompabilityTest;
    friend class IndexPartitionSchemaTest;
    friend class SchemaConfiguratorTest;
    friend class testlib::SchemaMaker;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IndexPartitionSchema);
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEXPARTITIONSCHEMA_H
