#ifndef __INDEXLIB_CUSTOMIZED_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_CUSTOMIZED_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class CustomizedIndexConfigImpl : public PackageIndexConfigImpl
{
public:
    CustomizedIndexConfigImpl(const std::string& indexName, IndexType indexType);
    CustomizedIndexConfigImpl(const CustomizedIndexConfigImpl& other); 
    ~CustomizedIndexConfigImpl();
public:
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override
    { AssertEqual(other); } 
    IndexConfigImpl* Clone() const override;

public:
    const std::string& GetIndexerName() const { return mIndexerName; }
    const util::KeyValueMap& GetParameters() const { return mParams; }

public: // for test    
    void SetIndexer(const std::string& indexerName) { mIndexerName = indexerName;}
    void SetParameters(const util::KeyValueMap& params) { mParams = params; }

protected:
    bool CheckFieldType(FieldType ft) const override
    {
        // support all field type
        return true;
    }

private:
    std::string mIndexerName;
    util::KeyValueMap mParams;
private:
    friend class CustomizedIndexConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_CONFIG_IMPL_H
