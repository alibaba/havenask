#include "indexlib/config/modify_item.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, ModifyItem);

ModifyItem::ModifyItem()
    : mOpId(INVALID_SCHEMA_OP_ID)
{
}

ModifyItem::ModifyItem(const string& name,
                       const string& type,
                       schema_opid_t opId)
    : mName(name)
    , mType(type)
    , mOpId(opId)
{
}

ModifyItem::~ModifyItem() 
{
}

void ModifyItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", mName);
    json.Jsonize("type", mType);

    if (json.GetMode() == TO_JSON)
    {
        if (mOpId != INVALID_SCHEMA_OP_ID)
        {
            json.Jsonize("operation_id", mOpId);
        }
    }
    else
    {
        json.Jsonize("operation_id", mOpId, mOpId);        
    }
}

IE_NAMESPACE_END(config);

