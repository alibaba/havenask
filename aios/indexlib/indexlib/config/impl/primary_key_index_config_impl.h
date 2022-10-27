#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/primary_key_load_strategy_param.h"

IE_NAMESPACE_BEGIN(config);

class PrimaryKeyIndexConfigImpl : public SingleFieldIndexConfigImpl
{
public:
    PrimaryKeyIndexConfigImpl(const std::string& indexName, IndexType indexType);
    PrimaryKeyIndexConfigImpl(const PrimaryKeyIndexConfigImpl& other);
    ~PrimaryKeyIndexConfigImpl();

public:
    IndexConfigImpl* Clone() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override;
    void Check() const override;

public:
    void SetPrimaryKeyLoadParam(    
        PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
        bool lookupReverse, const std::string& param = "");
    PrimaryKeyLoadStrategyParamPtr GetPKLoadStrategyParam() const
    {
        return mPkLoadParam;
    }

    void SetPrimaryKeyIndexType(PrimaryKeyIndexType type)
    { mPkIndexType = type; }
    
    PrimaryKeyIndexType GetPrimaryKeyIndexType() const
    { return mPkIndexType; }

    void SetPrimaryKeyHashType(PrimaryKeyHashType type)
    { mPkHashType = type; }
    
    PrimaryKeyHashType GetPrimaryKeyHashType() const
    { return mPkHashType;  }

protected:
    bool CheckFieldType(FieldType ft) const override;
    
private:
    void CheckPrimaryKey() const;
    bool IsPrimaryKeyIndex() const;
    
    static PrimaryKeyIndexType StringToPkIndexType(const std::string& strPkIndexType);
    static std::string PkIndexTypeToString(const PrimaryKeyIndexType type);
    static PrimaryKeyHashType StringToPkHashType(const std::string& strPkHashType);
    static std::string PkHashTypeToString(const PrimaryKeyHashType type);
private:
    PrimaryKeyLoadStrategyParamPtr mPkLoadParam;
    PrimaryKeyIndexType mPkIndexType;
    PrimaryKeyHashType mPkHashType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_IMPL_H
