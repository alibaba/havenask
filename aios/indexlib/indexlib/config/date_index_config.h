#ifndef __INDEXLIB_DATE_INDEX_CONFIG_H
#define __INDEXLIB_DATE_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/date_level_format.h"

DECLARE_REFERENCE_CLASS(config, DateIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class DateIndexConfig : public SingleFieldIndexConfig
{
public:
    DateIndexConfig(const std::string& indexName, IndexType indexType);
    DateIndexConfig(const DateIndexConfig& other);
    ~DateIndexConfig();
public:
    typedef DateLevelFormat::Granularity Granularity;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;
    IndexConfig* Clone() const override;
    Granularity GetBuildGranularity() const;
    DateLevelFormat GetDateLevelFormat() const;

public:
    void SetBuildGranularity(Granularity granularity);
    void SetDateLevelFormat(DateLevelFormat format);

private:
    DateIndexConfigImpl* mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DATE_INDEX_CONFIG_H
