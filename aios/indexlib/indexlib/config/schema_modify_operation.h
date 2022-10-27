#ifndef __INDEXLIB_SCHEMA_MODIFY_OPERATION_H
#define __INDEXLIB_SCHEMA_MODIFY_OPERATION_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/modify_item.h"

DECLARE_REFERENCE_CLASS(config, SchemaModifyOperationImpl);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(config);

class IndexPartitionSchemaImpl;
class SchemaModifyOperation : public autil::legacy::Jsonizable
{
public:
    SchemaModifyOperation();
    ~SchemaModifyOperation();
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    
    schema_opid_t GetOpId() const;
    void SetOpId(schema_opid_t id);

    void LoadDeleteOperation(const autil::legacy::Any &any,
                             IndexPartitionSchemaImpl& schema);
                             
    void LoadAddOperation(const autil::legacy::Any &any,
                          IndexPartitionSchemaImpl& schema);

    void MarkNotReady();
    bool IsNotReady() const;

    void CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs);
    void AssertEqual(const SchemaModifyOperation& other) const;

    const std::map<std::string, std::string>& GetParams() const;
    void SetParams(const std::map<std::string, std::string>& params);

    void Validate() const;
    
private:
    SchemaModifyOperationImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaModifyOperation);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SCHEMA_MODIFY_OPERATION_H
