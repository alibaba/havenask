#ifndef __INDEXLIB_RANGE_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_RANGE_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);
class RangeIndexConfig;
class RangeIndexConfigImpl : public SingleFieldIndexConfigImpl
{
public:
    RangeIndexConfigImpl(const std::string& indexName, IndexType indexType);
    ~RangeIndexConfigImpl();
public:
    void Check() const override;
    IndexConfigImpl* Clone() const override;
    IndexConfigPtr GetBottomLevelIndexConfig();
    IndexConfigPtr GetHighLevelIndexConfig();
    bool CheckFieldType(FieldType ft) const override;
private:
    IndexConfigPtr mBottomLevelConfig;
    IndexConfigPtr mHighLevelConfig;
private:
    friend class RangeIndexConfig;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_RANGE_INDEX_CONFIG_IMPL_H
