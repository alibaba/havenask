#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/impl/schema_modify_operation_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SchemaModifyOperation);

SchemaModifyOperation::SchemaModifyOperation()
{
    mImpl.reset(new SchemaModifyOperationImpl);
}

SchemaModifyOperation::~SchemaModifyOperation() 
{
}

void SchemaModifyOperation::Jsonize(Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

schema_opid_t SchemaModifyOperation::GetOpId() const
{
    return mImpl->GetOpId();
}

void SchemaModifyOperation::SetOpId(schema_opid_t id)
{
    mImpl->SetOpId(id);
}

void SchemaModifyOperation::LoadDeleteOperation(
        const Any &any, IndexPartitionSchemaImpl& schema)
{
    mImpl->LoadDeleteOperation(any, schema);
}
                             
void SchemaModifyOperation::LoadAddOperation(
        const Any &any, IndexPartitionSchemaImpl& schema)
{
    mImpl->LoadAddOperation(any, schema);
}

void SchemaModifyOperation::MarkNotReady()
{
    mImpl->MarkNotReady();
}

bool SchemaModifyOperation::IsNotReady() const
{
    return mImpl->IsNotReady();
}

void SchemaModifyOperation::CollectEffectiveModifyItem(
        ModifyItemVector& indexs, ModifyItemVector& attrs)
{
    return mImpl->CollectEffectiveModifyItem(indexs, attrs);
}

void SchemaModifyOperation::AssertEqual(const SchemaModifyOperation& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const map<string, string>& SchemaModifyOperation::GetParams() const
{
    return mImpl->GetParams();
}

void SchemaModifyOperation::SetParams(const map<string, string>& params)
{
    mImpl->SetParams(params);
}

void SchemaModifyOperation::Validate() const
{
    mImpl->Validate();
}

IE_NAMESPACE_END(config);

