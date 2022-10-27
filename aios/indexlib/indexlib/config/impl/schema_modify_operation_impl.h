#ifndef __INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H
#define __INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/modify_item.h"

DECLARE_REFERENCE_CLASS(testlib, ModifySchemaMaker);

IE_NAMESPACE_BEGIN(config);

class IndexPartitionSchemaImpl;
class SchemaModifyOperationImpl : public autil::legacy::Jsonizable
{
public:
    SchemaModifyOperationImpl();
    ~SchemaModifyOperationImpl();
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    schema_opid_t GetOpId() const;
    void SetOpId(schema_opid_t id);    

    void LoadDeleteOperation(const autil::legacy::Any &any,
                             IndexPartitionSchemaImpl& schema);
                             
    void LoadAddOperation(const autil::legacy::Any &any,
                          IndexPartitionSchemaImpl& schema);

    void MarkNotReady();
    bool IsNotReady() const { return mNotReady; }
    
    void CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs);

    void AssertEqual(const SchemaModifyOperationImpl& other) const;

    void Validate() const;

    const std::map<std::string, std::string>& GetParams() const
    { return mParameters; }
    
    void SetParams(const std::map<std::string, std::string>& params)
    { mParameters = params; }
    
private:
    void EnsureAccompanyAttributeDeleteForSpatialIndex(
            const std::string& indexName, IndexPartitionSchemaImpl& schema);

    void InitTruncateSortFieldSet(const IndexPartitionSchemaImpl& schema,
                                  std::set<std::string>& truncateSortFields);

private:
    std::vector<std::string> mDeleteFields;
    std::vector<std::string> mDeleteIndexs;
    std::vector<std::string> mDeleteAttrs;

    std::vector<FieldConfigPtr> mAddFields;
    std::vector<IndexConfigPtr> mAddIndexs;
    std::vector<AttributeConfigPtr> mAddAttrs;
    
    schema_opid_t mOpId;
    bool mNotReady; /* add op not ready, delete op take effect immediately */
    
    std::map<std::string, std::string> mParameters;
    
private:
    friend class testlib::ModifySchemaMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaModifyOperationImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H
