#ifndef __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H
#define __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"

DECLARE_REFERENCE_CLASS(config, SingleFieldIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class SingleFieldIndexConfig : public IndexConfig
{
public:
    SingleFieldIndexConfig(const std::string& indexName, IndexType indexType);
    SingleFieldIndexConfig(const SingleFieldIndexConfig& other);
    ~SingleFieldIndexConfig();

public:
    uint32_t GetFieldCount() const override;
    void Check() const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;

    IndexConfig::Iterator CreateIterator() const override;
    int32_t GetFieldIdxInPack(fieldid_t id) const override;

protected:
    bool CheckFieldType(FieldType ft) const override;
    void ResetImpl(IndexConfigImpl* impl);

public:
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);
    FieldConfigPtr GetFieldConfig() const;

    //TODO: outsize used in plugin, expect merge to primarykey config
    bool HasPrimaryKeyAttribute() const;
    void SetPrimaryKeyAttributeFlag(bool flag);

private:
    SingleFieldIndexConfigImpl* mImpl;
    
private:
    friend class SingleFieldIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_H
