#ifndef __INDEXLIB_PRIMARY_KEY_SEGMENT_FORMATTER_H
#define __INDEXLIB_PRIMARY_KEY_SEGMENT_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/config/primary_key_index_config.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeySegmentFormatter
{
public:
    typedef std::tr1::shared_ptr<PrimaryKeyFormatter<Key> > PrimaryKeyFormatterPtr;
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::tr1::shared_ptr<HashMapType> HashMapTypePtr;

public:
    PrimaryKeySegmentFormatter() {}
    ~PrimaryKeySegmentFormatter() {}
public:
    void Init(const config::PrimaryKeyIndexConfigPtr& indexConfig)
    {
        mPrimaryKeyFormatter = 
            PrimaryKeyFormatterCreator<Key>::CreatePrimaryKeyFormatter(indexConfig);
        mLoadMode = indexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
        if (mLoadMode == config::PrimaryKeyLoadStrategyParam::HASH_TABLE)
        {
            mHashPrimaryKeyFormatter = dynamic_cast<HashPrimaryKeyFormatter<Key>* >(mPrimaryKeyFormatter.get());
        }
    }

    docid_t Find(char* data, size_t count, Key key) const __ALWAYS_INLINE;

private:
    PrimaryKeyFormatterPtr mPrimaryKeyFormatter;
    HashPrimaryKeyFormatter<Key>* mHashPrimaryKeyFormatter;
    config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode mLoadMode;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////
template <typename Key>
inline docid_t PrimaryKeySegmentFormatter<Key>::Find(
        char* data, size_t count, Key key) const
{
    if (mLoadMode == config::PrimaryKeyLoadStrategyParam::HASH_TABLE)
    {
        return mHashPrimaryKeyFormatter->Find(data, key);
    }
    return SortedPrimaryKeyFormatter<Key>::Find((PKPair*)data, count, key);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_SEGMENT_FORMATTER_H
