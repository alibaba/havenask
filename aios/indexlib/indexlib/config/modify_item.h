#ifndef __INDEXLIB_MODIFY_ITEM_H
#define __INDEXLIB_MODIFY_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(config);

class ModifyItem : public autil::legacy::Jsonizable
{
public:
    ModifyItem();
    ModifyItem(const std::string& name,
               const std::string& type, schema_opid_t opId);
    ~ModifyItem();
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetName() const { return mName; }
    const std::string& GetType() const { return mType; }
    schema_opid_t GetModifyOperationId() const { return mOpId; }
    
private:
    std::string mName;
    std::string mType;
    schema_opid_t mOpId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModifyItem);
typedef std::vector<ModifyItemPtr> ModifyItemVector;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MODIFY_ITEM_H
