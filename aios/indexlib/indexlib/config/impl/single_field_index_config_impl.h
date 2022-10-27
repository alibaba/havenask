#ifndef __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/impl/index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class SingleFieldIndexConfigImpl : public IndexConfigImpl
{
public:
    SingleFieldIndexConfigImpl(const std::string& indexName, IndexType indexType);
    SingleFieldIndexConfigImpl(const SingleFieldIndexConfigImpl& other);
    ~SingleFieldIndexConfigImpl() {}

public:
    uint32_t GetFieldCount() const override { return 1; }
    void Check() const override;
    bool IsInIndex(fieldid_t fieldId) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override;
    IndexConfigImpl* Clone() const override;

    IndexConfig::Iterator CreateIterator() const override 
    { return IndexConfig::Iterator(mFieldConfig); }
    int32_t GetFieldIdxInPack(fieldid_t id) const override
    { 
        fieldid_t fieldId = mFieldConfig->GetFieldId();
        return fieldId == id ? 0 : -1;
    }

protected:
    bool CheckFieldType(FieldType ft) const override;

public:
    void SetFieldConfig(const FieldConfigPtr& fieldConfig);
    FieldConfigPtr GetFieldConfig() const { return mFieldConfig; }

    //TODO: outsize used in plugin, expect merge to primarykey config
    bool HasPrimaryKeyAttribute() const { return mHasPrimaryKeyAttribute; }
    void SetPrimaryKeyAttributeFlag(bool flag) { mHasPrimaryKeyAttribute = flag; }

private:
    void CheckWhetherIsVirtualField() const;

protected:
    FieldConfigPtr mFieldConfig;
    bool mHasPrimaryKeyAttribute;

private:
    friend class SingleFieldIndexConfigTest;
    friend class SingleFieldIndexConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SINGLE_FIELD_INDEX_CONFIG_IMPL_H
