#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/primary_key_load_strategy_param.h"

DECLARE_REFERENCE_CLASS(config, PrimaryKeyIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class PrimaryKeyIndexConfig : public SingleFieldIndexConfig
{
public:
    PrimaryKeyIndexConfig(const std::string& indexName, IndexType indexType);
    PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other);
    ~PrimaryKeyIndexConfig();

public:
    IndexConfig* Clone() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;

public:
    void SetPrimaryKeyLoadParam(    
        PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
        bool lookupReverse, const std::string& param = "");
    PrimaryKeyLoadStrategyParamPtr GetPKLoadStrategyParam() const;
    void SetPrimaryKeyIndexType(PrimaryKeyIndexType type);    
    PrimaryKeyIndexType GetPrimaryKeyIndexType() const;
    void SetPrimaryKeyHashType(PrimaryKeyHashType type);
    PrimaryKeyHashType GetPrimaryKeyHashType() const;

private:
    PrimaryKeyIndexConfigImpl* mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_CONFIG_H
