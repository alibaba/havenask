#ifndef __INDEXLIB_DISABLE_FIELDS_CONFIG_H
#define __INDEXLIB_DISABLE_FIELDS_CONFIG_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class DisableFieldsConfig : public autil::legacy::Jsonizable
{
public:
    DisableFieldsConfig();
    ~DisableFieldsConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator == (const DisableFieldsConfig& other) const;

public:
    std::vector<std::string> attributes;
    std::vector<std::string> indexes;
    std::vector<std::string> packAttributes;
    bool rewriteLoadConfig = true;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DisableFieldsConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DISABLE_FIELD_LIST_H
