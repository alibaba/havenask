#ifndef __INDEXLIB_DATE_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_DATE_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/date_level_format.h"

IE_NAMESPACE_BEGIN(config);

class DateIndexConfigImpl : public SingleFieldIndexConfigImpl
{
public:
    DateIndexConfigImpl(const std::string& indexName, IndexType indexType);
    DateIndexConfigImpl(const DateIndexConfigImpl& other);
    ~DateIndexConfigImpl();
public:
    typedef DateLevelFormat::Granularity Granularity;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override
    { AssertEqual(other); }
    void Check() const override;
    IndexConfigImpl* Clone() const override;
    Granularity GetBuildGranularity() const
    { return mBuildGranularity; }
    DateLevelFormat GetDateLevelFormat() const { return mDateLevelFormat; }

public:
    void SetBuildGranularity(Granularity granularity) { mBuildGranularity = granularity; }
    void SetDateLevelFormat(DateLevelFormat format) { mDateLevelFormat = format; }
private:
    std::string GranularityToString(Granularity granularity);
    Granularity StringToGranularity(const std::string& str);
private:
    Granularity mBuildGranularity;
    DateLevelFormat mDateLevelFormat;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DATE_INDEX_CONFIG_IMPL_H
