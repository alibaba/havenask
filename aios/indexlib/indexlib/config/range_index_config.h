#ifndef __INDEXLIB_RANGE_INDEX_CONFIG_H
#define __INDEXLIB_RANGE_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"

DECLARE_REFERENCE_CLASS(config, RangeIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class RangeIndexConfig : public SingleFieldIndexConfig
{
public:
    RangeIndexConfig(const std::string& indexName, IndexType indexType);
    RangeIndexConfig(const RangeIndexConfig& other);
    RangeIndexConfig(RangeIndexConfigImpl* impl);
    ~RangeIndexConfig();
public:
    void Check() const override;
    IndexConfig* Clone() const override;
    static std::string GetBottomLevelIndexName(const std::string& indexName);
    static std::string GetHighLevelIndexName(const std::string& indexName);
    IndexConfigPtr GetBottomLevelIndexConfig();
    IndexConfigPtr GetHighLevelIndexConfig();
    bool CheckFieldType(FieldType ft) const override;
private:
    RangeIndexConfigImpl* mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_RANGE_INDEX_CONFIG_H
