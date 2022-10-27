#ifndef __INDEXLIB_CUSTOMIZED_INDEX_CONFIG_H
#define __INDEXLIB_CUSTOMIZED_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/config/package_index_config.h"

DECLARE_REFERENCE_CLASS(config, CustomizedIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class CustomizedIndexConfig : public PackageIndexConfig
{
public:
    CustomizedIndexConfig(const std::string& indexName, IndexType indexType);
    CustomizedIndexConfig(const CustomizedIndexConfig& other); 
    ~CustomizedIndexConfig();
public:
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;

public:
    const std::string& GetIndexerName() const;
    const util::KeyValueMap& GetParameters() const;

public: // for test    
    void SetIndexer(const std::string& indexerName);
    void SetParameters(const util::KeyValueMap& params);

protected:
    bool CheckFieldType(FieldType ft) const override;
    
private:
    CustomizedIndexConfigImpl* mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_CONFIG_H
