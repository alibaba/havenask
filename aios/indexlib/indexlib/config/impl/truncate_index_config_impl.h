#ifndef __INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/config/truncate_index_property.h"

IE_NAMESPACE_BEGIN(config);

class TruncateIndexConfigImpl 
{
public:
    TruncateIndexConfigImpl();
    ~TruncateIndexConfigImpl();
public:
    const std::string& GetIndexName() const { return mIndexName; }
    void SetIndexName(const std::string& indexName)  
    { 
        mIndexName = indexName; 
    }

    void AddTruncateIndexProperty(const TruncateIndexProperty& property)
    {
        mTruncIndexProperties.push_back(property);
    }

    const TruncateIndexPropertyVector& GetTruncateIndexProperties() const 
    { return mTruncIndexProperties; }

    const TruncateIndexProperty& GetTruncateIndexProperty(size_t i) const
    {
        assert(i < mTruncIndexProperties.size());
        return mTruncIndexProperties[i];
    }

    TruncateIndexProperty& GetTruncateIndexProperty(size_t i)
    {
        assert(i < mTruncIndexProperties.size());
        return mTruncIndexProperties[i];
    }

    size_t GetTruncateIndexPropertySize() const
    {
        return mTruncIndexProperties.size();
    }
private:
    std::string mIndexName;
    TruncateIndexPropertyVector mTruncIndexProperties;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexProperty);
DEFINE_SHARED_PTR(TruncateIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H
