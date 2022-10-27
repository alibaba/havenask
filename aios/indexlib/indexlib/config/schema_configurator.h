#ifndef __INDEXLIB_SCHEMACONFIGURATOR_H
#define __INDEXLIB_SCHEMACONFIGURATOR_H

#include <string>
#include <autil/legacy/json.h>
#include <autil/legacy/any.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchemaImpl);
DECLARE_REFERENCE_CLASS(config, DictionarySchema);
DECLARE_REFERENCE_CLASS(config, DictionaryConfig);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, RegionSchema);
DECLARE_REFERENCE_CLASS(config, AdaptiveDictionarySchema);
DECLARE_REFERENCE_CLASS(testlib, ModifySchemaMaker);

IE_NAMESPACE_BEGIN(config);

class SchemaConfigurator
{
public:
    void Configurate(
        const autil::legacy::Any& any,
        IndexPartitionSchemaImpl& schema,
        bool isSubSchema);
    
private:
    AdaptiveDictionarySchemaPtr LoadAdaptiveDictSchema(const autil::legacy::Any& any,
                                                       IndexPartitionSchemaImpl& schema);
    DictionarySchemaPtr LoadDictSchema(const autil::legacy::Any& any,
                                       IndexPartitionSchemaImpl& schema);
    DictionaryConfigPtr LoadDictionaryConfig(const autil::legacy::Any& any,
                                             IndexPartitionSchemaImpl& schema);

    void LoadRegions(const autil::legacy::Any& any,
                     IndexPartitionSchemaImpl& schema);

    RegionSchemaPtr LoadRegionSchema(const autil::legacy::Any& any,
                                     IndexPartitionSchemaImpl& schema,
                                     bool multiRegionFormat);

    void LoadModifyOperations(const autil::legacy::Any& any,
                              IndexPartitionSchemaImpl& schema);

    static void LoadOneModifyOperation(const autil::legacy::Any& any,
            IndexPartitionSchemaImpl& schema);

    void LoadCustomizedConfig(const autil::legacy::Any& any,
                              IndexPartitionSchemaImpl& schema);
    static void AddOfflineJoinVirtualAttribute(
        const std::string& attrName, 
        IndexPartitionSchemaImpl& destSchema);
    
private:
    friend class testlib::ModifySchemaMaker;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SCHEMACONFIGURATOR_H
