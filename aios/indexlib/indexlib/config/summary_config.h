#ifndef __INDEXLIB_SUMMARY_CONFIG_H
#define __INDEXLIB_SUMMARY_CONFIG_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "field_config.h"
#include <cassert>

IE_NAMESPACE_BEGIN(config);
class SummaryConfigImpl;
DEFINE_SHARED_PTR(SummaryConfigImpl);

class SummaryConfig
{
public:
    SummaryConfig();
    ~SummaryConfig();
public:
    FieldType GetFieldType() const;
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);
    FieldConfigPtr GetFieldConfig() const;
    fieldid_t GetFieldId() const;
    const std::string& GetSummaryName() const;
    void AssertEqual(const SummaryConfig& other) const;
    
private:
    SummaryConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_CONFIG_H
