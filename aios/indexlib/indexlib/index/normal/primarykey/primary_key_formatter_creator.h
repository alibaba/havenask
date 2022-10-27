#ifndef __INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H
#define __INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/config/primary_key_index_config.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyFormatterCreator
{
public:
    typedef std::tr1::shared_ptr<PrimaryKeyFormatter<Key> > PrimaryKeyFormatterPtr;

public:
    PrimaryKeyFormatterCreator() {}
    ~PrimaryKeyFormatterCreator() {}

public:
    static PrimaryKeyFormatterPtr CreatePrimaryKeyFormatter(
            const config::PrimaryKeyIndexConfigPtr& indexConfig);

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////
template<typename Key>
inline typename PrimaryKeyFormatterCreator<Key>::PrimaryKeyFormatterPtr
PrimaryKeyFormatterCreator<Key>::CreatePrimaryKeyFormatter(
        const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    // TODO: only for read, move to pk loader
    config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode = 
        indexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
    if (loadMode == config::PrimaryKeyLoadStrategyParam::HASH_TABLE)
    {
        return PrimaryKeyFormatterPtr(new HashPrimaryKeyFormatter<Key>());
    }

    assert(loadMode == config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR);
    assert(indexConfig->GetPrimaryKeyIndexType() == pk_sort_array);
    return PrimaryKeyFormatterPtr(new SortedPrimaryKeyFormatter<Key>());
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_FORMATTER_CREATOR_H
