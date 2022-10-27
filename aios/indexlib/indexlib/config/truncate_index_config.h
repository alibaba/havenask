#ifndef __INDEXLIB_TRUNCATE_INDEX_CONFIG_H
#define __INDEXLIB_TRUNCATE_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/config/truncate_index_property.h"

DECLARE_REFERENCE_CLASS(config, TruncateIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class TruncateIndexConfig 
{
public:
    TruncateIndexConfig();
    ~TruncateIndexConfig();
public:
    const std::string& GetIndexName() const;
    void SetIndexName(const std::string& indexName);
    void AddTruncateIndexProperty(const TruncateIndexProperty& property);

    const TruncateIndexPropertyVector& GetTruncateIndexProperties() const;

    const TruncateIndexProperty& GetTruncateIndexProperty(size_t i) const;

    TruncateIndexProperty& GetTruncateIndexProperty(size_t i);
    
    size_t GetTruncateIndexPropertySize() const;
    
private:
    TruncateIndexConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexProperty);
DEFINE_SHARED_PTR(TruncateIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_INDEX_CONFIG_H
