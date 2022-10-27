#ifndef __INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H
#define __INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_formatter.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeySegmentReaderTyped : public index::IndexSegmentReader
{
public:
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;
    typedef typename std::tr1::shared_ptr<PrimaryKeyPairSegmentIterator<Key> > PkPairIteratorPtr;

public:
    PrimaryKeySegmentReaderTyped() 
        : mData(NULL)
        , mItemCount(0)
    {}

    ~PrimaryKeySegmentReaderTyped() {}

public:
    void Init(const config::PrimaryKeyIndexConfigPtr& indexConfig,
              const file_system::FileReaderPtr& mFileReader);

    docid_t Lookup(const Key& hashKey) const __ALWAYS_INLINE;

    PkPairIteratorPtr CreateIterator() const
    {
        PkPairIteratorPtr iterator(new SortedPrimaryKeyPairSegmentIterator<Key>());
        if (mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode() == 
            config::PrimaryKeyLoadStrategyParam::SORTED_VECTOR)
        {
            file_system::FileReaderPtr fileReader(
                new file_system::NormalFileReader(mFileReader->GetFileNode()));
            iterator->Init(fileReader);
            return iterator;
        }
        INDEXLIB_FATAL_ERROR(UnSupported, "Not support create hash iterator");
        return PkPairIteratorPtr();
    }

private:
    char* mData;
    uint32_t mItemCount;
    file_system::FileReaderPtr mFileReader;
    config::PrimaryKeyIndexConfigPtr mIndexConfig;
    PrimaryKeySegmentFormatter<Key> mPkFormatter;

private:
    IE_LOG_DECLARE();
};

typedef PrimaryKeySegmentReaderTyped<uint64_t> UInt64PrimaryKeySegmentReader;
DEFINE_SHARED_PTR(UInt64PrimaryKeySegmentReader);

typedef PrimaryKeySegmentReaderTyped<autil::uint128_t> UInt128PrimaryKeySegmentReader;
DEFINE_SHARED_PTR(UInt128PrimaryKeySegmentReader);

////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, PrimaryKeySegmentReaderTyped);

template<typename Key>
inline void PrimaryKeySegmentReaderTyped<Key>::Init(
        const config::PrimaryKeyIndexConfigPtr& indexConfig,
        const file_system::FileReaderPtr& fileReader)
{
    mIndexConfig = indexConfig;
    mFileReader = fileReader;
    mData = (char*)fileReader->GetBaseAddress();
    mPkFormatter.Init(indexConfig);
    //TODO: pkhash not right
    mItemCount = fileReader->GetLength() / sizeof(PKPair);
}

template<typename Key>
inline docid_t PrimaryKeySegmentReaderTyped<Key>::Lookup(const Key& hashKey) const
{
    return mPkFormatter.Find(mData, mItemCount, hashKey);
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_SEGMENT_READER_TYPED_H
