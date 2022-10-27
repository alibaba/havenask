#ifndef __INDEXLIB_SUMMARY_CONFIG_IMPL_H
#define __INDEXLIB_SUMMARY_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/config/field_config.h"
#include "indexlib/config/summary_config.h"
#include <cassert>

IE_NAMESPACE_BEGIN(config);

class SummaryConfigImpl
{
public:
    SummaryConfigImpl() {}
    ~SummaryConfigImpl() {}
public:
    FieldType GetFieldType() const 
    {
        assert(mFieldConfig);
        return mFieldConfig->GetFieldType();
    }

    void SetFieldConfig(const FieldConfigPtr& fieldConfig) 
    {
        assert(fieldConfig);
        mFieldConfig = fieldConfig;
    }
    
    FieldConfigPtr GetFieldConfig() const
    { return mFieldConfig; }

    fieldid_t GetFieldId() const
    { return mFieldConfig->GetFieldId(); }

    const std::string& GetSummaryName() const
    { return mFieldConfig->GetFieldName(); }

    void AssertEqual(const SummaryConfigImpl& other) const;
    
private:
    FieldConfigPtr mFieldConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_CONFIG_IMPL_H
