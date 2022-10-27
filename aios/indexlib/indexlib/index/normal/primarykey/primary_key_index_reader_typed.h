#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_READER_TYPED_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_READER_TYPED_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include <autil/LongHashValue.h>
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/common_define.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_posting_iterator.h"
#include "indexlib/index/normal/attribute/accessor/primary_key_attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_convertor.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/primary_key_load_strategy.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"
#include "indexlib/index/normal/primarykey/primary_key_load_strategy_creator.h"
#include "indexlib/index/normal/primarykey/primary_key_building_index_reader.h"
#include "indexlib/config/primary_key_index_config.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyIndexReaderTyped : public PrimaryKeyIndexReader
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::tr1::shared_ptr<HashMapTyped> HashMapTypedPtr;

public:
    typedef PrimaryKeySegmentReaderTyped<Key> SegmentReader;
    typedef std::tr1::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef std::pair<docid_t, SegmentReaderPtr> PrimaryKeySegment;
    typedef std::vector<PrimaryKeySegment> PKSegmentList;

    typedef PrimaryKeyAttributeReader<Key> PKAttributeReaderType;
    typedef std::tr1::shared_ptr<PKAttributeReaderType> PKAttributeReaderTypePtr;
    typedef std::tr1::shared_ptr<PrimaryKeyBuildingIndexReader<Key> > PrimaryKeyBuildingReaderPtr;
    typedef std::tr1::shared_ptr<InMemPrimaryKeySegmentReaderTyped<Key> > InMemPrimaryKeySegmentReaderPtr;

public:
    PrimaryKeyIndexReaderTyped() {}
    ~PrimaryKeyIndexReaderTyped() {}

public:
    // // TODO: remove
    PrimaryKeyIndexReaderTyped(const PrimaryKeyIndexReaderTyped& indexReader);
    IndexReader* Clone() const override;

public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData) override;
    void OpenWithoutPKAttribute(const config::IndexConfigPtr& indexConfig,
                                const index_base::PartitionDataPtr& partitionData) override;

    AttributeReaderPtr GetPKAttributeReader() const override
    { return mPKAttributeReader; }

    PostingIterator *Lookup(const common::Term& term, 
                            uint32_t statePoolSize = 1000, PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL) override;
    docid_t Lookup(const autil::ConstString& pkStr) const override;
    docid_t Lookup(const std::string& strKey) const override;
    docid_t LookupWithNumber(uint64_t pk) const override ;
    docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const override;
    size_t EstimateLoadSize(
            const index_base::PartitionDataPtr& partitionData,
            const config::IndexConfigPtr& indexConfig,
            const index_base::Version& lastLoadVersion) override;
    bool LookupAll(const std::string& pkStr,
                   std::vector<std::pair<docid_t, bool>>& docidPairVec) const override;

public:
    docid_t Lookup(const Key& key) const __ALWAYS_INLINE;
    docid_t Lookup(const Key& key, docid_t& lastDocId) const;
    docid_t Lookup(const std::string& pkStr, docid_t& lastDocId) const;

    template<typename T>
    inline docid_t LookupWithType(const T& key) const __ALWAYS_INLINE;
    docid_t LookupWithType(const std::string& key) const __ALWAYS_INLINE;

    OnDiskOrderedPrimaryKeyIterator<Key> CreateOnDiskOrderedIterator() const;
    static std::string Identifier();

    const util::KeyHasherPtr& GetHasher() const { return mHashFunc; }

private:
    void InitBuildingIndexReader(const index_base::PartitionDataPtr& partitionData);
    bool IsDocIdValid(docid_t docid) const __ALWAYS_INLINE;
    docid_t LookupInMemorySegment(const Key& hashKey) const;
    docid_t LookupOnDiskSegments(const Key& hashKey, docid_t& lastDocId) const __ALWAYS_INLINE;
    docid_t LookupOnDiskSegments(const Key& hashKey) const __ALWAYS_INLINE;
    docid_t LookupOneSegment(const Key& hashKey, docid_t baseDocid,
                             const SegmentReaderPtr& segReader) const __ALWAYS_INLINE;

protected:
    // protected for fake
    bool Hash(const std::string& keyStr, Key& key) const
    { return mHashFunc->GetHashKey(keyStr.c_str(), keyStr.size(), key); }

protected:
    PKAttributeReaderTypePtr mPKAttributeReader;
    PKSegmentList mSegmentList;
    util::KeyHasherPtr mHashFunc;
    bool mIsNumberHash;
    bool mNeedLookupReverse;
    config::PrimaryKeyIndexConfigPtr mPrimaryKeyIndexConfig;
    DeletionMapReaderPtr mDeletionMapReader;
    PrimaryKeyBuildingReaderPtr mBuildingIndexReader;
    
private:
    friend class PrimaryKeyIndexReaderTypedTest;
    friend class PrimaryKeyIndexReaderTypedIntetestTest;
    IE_LOG_DECLARE();
};

typedef PrimaryKeyIndexReaderTyped<uint64_t> UInt64PrimaryKeyIndexReader;
typedef PrimaryKeyIndexReaderTyped<autil::uint128_t> UInt128PrimaryKeyIndexReader;
DEFINE_SHARED_PTR(UInt64PrimaryKeyIndexReader);
DEFINE_SHARED_PTR(UInt128PrimaryKeyIndexReader);

//////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyIndexReaderTyped);
template<typename Key>
inline PrimaryKeyIndexReaderTyped<Key>::PrimaryKeyIndexReaderTyped(
        const PrimaryKeyIndexReaderTyped<Key>& indexReader)
    : mSegmentList(indexReader.mSegmentList)
    , mHashFunc(indexReader.mHashFunc)
    , mIsNumberHash(indexReader.mIsNumberHash)
    , mNeedLookupReverse(indexReader.mNeedLookupReverse)
    , mPrimaryKeyIndexConfig(indexReader.mPrimaryKeyIndexConfig)
    , mDeletionMapReader(indexReader.mDeletionMapReader)
    , mBuildingIndexReader(indexReader.mBuildingIndexReader)
{
    if (indexReader.mPKAttributeReader)
    {
        PKAttributeReaderType* pkAttrReader = 
            dynamic_cast<PKAttributeReaderType*>(
                    indexReader.mPKAttributeReader->Clone());
        assert(pkAttrReader);
        mPKAttributeReader.reset(pkAttrReader);
    }
}

template<typename Key>
inline void PrimaryKeyIndexReaderTyped<Key>::Open(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    mIndexConfig = indexConfig;
    OpenWithoutPKAttribute(mIndexConfig, partitionData);

    assert(mPrimaryKeyIndexConfig);
    if (mPrimaryKeyIndexConfig->HasPrimaryKeyAttribute())
    {
        mPKAttributeReader.reset(new PKAttributeReaderType(
                        mPrimaryKeyIndexConfig->GetIndexName()));
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig());
        attrConfig->Init(mPrimaryKeyIndexConfig->GetFieldConfig());
        mPKAttributeReader->Open(attrConfig, partitionData);
    }
}

inline bool CompByDocCount(const PrimaryKeyLoadPlanPtr& leftPlan,
                    const PrimaryKeyLoadPlanPtr& rightPlan)
{
    return (leftPlan->GetDocCount() - leftPlan->GetDeletedDocCount()) > 
        (rightPlan->GetDocCount() - rightPlan->GetDeletedDocCount());
}

inline bool CompByBaseDocId(const PrimaryKeyLoadPlanPtr& leftPlan,
                     const PrimaryKeyLoadPlanPtr& rightPlan)
{
    return leftPlan->GetBaseDocId() > rightPlan->GetBaseDocId();
}

template<typename Key>
inline void PrimaryKeyIndexReaderTyped<Key>::OpenWithoutPKAttribute(
        const config::IndexConfigPtr& indexConfig, 
        const index_base::PartitionDataPtr& partitionData)
{
    assert(indexConfig);
    
    mPrimaryKeyIndexConfig = DYNAMIC_POINTER_CAST(
            config::PrimaryKeyIndexConfig, indexConfig);
    assert(mPrimaryKeyIndexConfig);
    PrimaryKeyLoadStrategyPtr strategy = 
        PrimaryKeyLoadStrategyCreator::CreatePrimaryKeyLoadStrategy(
                mPrimaryKeyIndexConfig);
    PrimaryKeyLoadPlanVector plans;
    strategy->CreatePrimaryKeyLoadPlans(partitionData, plans);

    mNeedLookupReverse = mPrimaryKeyIndexConfig->GetPKLoadStrategyParam()->NeedLookupReverse();
    if (mNeedLookupReverse)
    {
        std::sort(plans.begin(), plans.end(), CompByBaseDocId);
    }
    else
    {
        std::sort(plans.begin(), plans.end(), CompByDocCount);
    }

    for (size_t i = 0; i < plans.size(); i++)
    {
        IE_LOG(DEBUG, "primary key load plan target [%s]",
               plans[i]->GetTargetFileName().c_str());
        //TODO: mv open fileReader to segmentReader
        file_system::FileReaderPtr fileReader = 
            PrimaryKeyLoader<Key>::Load(plans[i], mPrimaryKeyIndexConfig);
        SegmentReaderPtr segReader(new SegmentReader);
        segReader->Init(mPrimaryKeyIndexConfig, fileReader);
        mSegmentList.push_back(std::make_pair(plans[i]->GetBaseDocId(), segReader));
    }

    InitBuildingIndexReader(partitionData);

    try
    {
        mDeletionMapReader = partitionData->GetDeletionMapReader();
    }
    catch (misc::UnSupportedException& e)
    {
        mDeletionMapReader.reset();
        IE_LOG(INFO, "Merger reclaim document will use MergePartitionData, "
               "which not support get deletionMapReader.");
    }

    config::FieldConfigPtr fieldConfig = mPrimaryKeyIndexConfig->GetFieldConfig();
    mIsNumberHash = mPrimaryKeyIndexConfig->GetPrimaryKeyHashType() == pk_number_hash;
    mHashFunc.reset(util::KeyHasherFactory::CreatePrimaryKeyHasher(
                    fieldConfig->GetFieldType(),
                    mPrimaryKeyIndexConfig->GetPrimaryKeyHashType()));

}

// parameter @type is ignored
template<typename Key>
inline PostingIterator *PrimaryKeyIndexReaderTyped<Key>::Lookup(
        const common::Term& term, uint32_t statePoolSize, 
        PostingType type, autil::mem_pool::Pool *sessionPool)
{
    Key hashKey;    
    dictkey_t termHashKey;
    if (term.GetTermHashKey(termHashKey))
    {
        hashKey = (Key)termHashKey;
    }
    else if (!Hash(term.GetWord(), hashKey))
    {
        return NULL;
    }

    docid_t docId = Lookup(hashKey);
    if (docId != INVALID_DOCID)
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, PrimaryKeyPostingIterator,
                docId, sessionPool);
    }
    return NULL;
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::Lookup(
        const std::string& strKey) const
{
    return LookupWithType(strKey);
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::Lookup(const autil::ConstString& pkStr) const
{
   Key key;
   if (!mHashFunc->GetHashKey(pkStr.data(), pkStr.size(), key))
   {
       return INVALID_DOCID;
   }
   return Lookup(key);
}

template<>
inline docid_t PrimaryKeyIndexReaderTyped<autil::uint128_t>::LookupWithPKHash(
        const autil::uint128_t& pkHash) const
{
    return Lookup(pkHash);
}

template<>
inline docid_t PrimaryKeyIndexReaderTyped<uint64_t>::LookupWithPKHash(
        const autil::uint128_t& pkHash) const
{
    uint64_t pkValue;
    PrimaryKeyHashConvertor::ToUInt64(pkHash, pkValue);
    return Lookup(pkValue);
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::Lookup(
        const std::string& pkStr, docid_t& lastDocId) const
{
    Key hashKey;
    if (!Hash(pkStr, hashKey))
    {
        return INVALID_DOCID;
    }
    return Lookup(hashKey, lastDocId);
}

template<typename Key>
docid_t PrimaryKeyIndexReaderTyped<Key>::LookupInMemorySegment(
        const Key& hashKey) const
{
    if (!mBuildingIndexReader)
    {
        return INVALID_DOCID;
    }
    return mBuildingIndexReader->Lookup(hashKey);
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::LookupOnDiskSegments(
        const Key& hashKey) const
{
    for (size_t i = 0; i < mSegmentList.size(); i++)
    {
        docid_t gDocId = LookupOneSegment(hashKey, mSegmentList[i].first, mSegmentList[i].second);
        if (!IsDocIdValid(gDocId))
        {
            //for inc cover rt, rt doc deleted use inc doc
            continue;
        }
        return gDocId;
    }
    return INVALID_DOCID;
}

template<typename Key>
inline bool PrimaryKeyIndexReaderTyped<Key>::IsDocIdValid(docid_t docid) const
{
    if (docid == INVALID_DOCID)
    {
        return false;
    }

    if (mDeletionMapReader && mDeletionMapReader->IsDeleted(docid))
    {
        return false;
    }
    return true;
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::LookupOneSegment(
    const Key& hashKey, docid_t baseDocid, const SegmentReaderPtr& segReader) const
{
    docid_t localDocId = segReader->Lookup(hashKey);
    if (localDocId == INVALID_DOCID)
    {
        return INVALID_DOCID;
    }
    return baseDocid + localDocId;
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::LookupOnDiskSegments(
        const Key& hashKey, docid_t& lastDocId) const
{
    for (size_t i = 0; i < mSegmentList.size(); i++)
    {
        docid_t gDocId = LookupOneSegment(hashKey, mSegmentList[i].first,
                                          mSegmentList[i].second);
        if (lastDocId < gDocId)
        {
            lastDocId = gDocId;
        }
        if (!IsDocIdValid(gDocId))
        {
            //for inc cover rt, rt doc deleted use inc doc
            continue;
        }
        lastDocId = gDocId;
        return gDocId;
    }
    return INVALID_DOCID;
}

template <typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::LookupWithNumber(uint64_t pk) const
{
    return LookupWithType(pk);
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::Lookup(const Key& hashKey) const
{
    docid_t docId = INVALID_DOCID;
    if (mNeedLookupReverse)
    {
        docId = LookupInMemorySegment(hashKey);
        if (IsDocIdValid(docId))
        {
            return docId;
        }
    }

    docId = LookupOnDiskSegments(hashKey);
    if (docId != INVALID_DOCID)
    {
        return docId;
    }

    if (!mNeedLookupReverse)
    {
        docId = LookupInMemorySegment(hashKey);
        if (IsDocIdValid(docId))
        {
            return docId;
        }
    }
    return INVALID_DOCID;
}

template<typename Key>
docid_t PrimaryKeyIndexReaderTyped<Key>::Lookup(const Key& hashKey, docid_t& lastDocId) const
{
    // look up in memory segment reader first
    docid_t docId = LookupInMemorySegment(hashKey);
    lastDocId = docId;//INVALID or building docid
    if (IsDocIdValid(docId))
    {
        return docId;
    }

    return LookupOnDiskSegments(hashKey, lastDocId);
}

template<typename Key>
inline std::string PrimaryKeyIndexReaderTyped<Key>::Identifier() 
{
    if (typeid(Key) == typeid(uint64_t))
    {
        return "primary_key.uint64.reader";
    }
    else if (typeid(Key) == typeid(autil::uint128_t))
    {
        return "primary_key.uint128.reader";
    }

    return "primary_key.typed.reader";
}

template<typename Key>
inline IndexReader* PrimaryKeyIndexReaderTyped<Key>::Clone() const
{
    return new PrimaryKeyIndexReaderTyped<Key>(*this);
}

template<typename Key>
inline void PrimaryKeyIndexReaderTyped<Key>::InitBuildingIndexReader(
        const index_base::PartitionDataPtr& partitionData)
{
    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr buildingSegIter =
        segIter->CreateIterator(index_base::SIT_BUILDING);
    while (buildingSegIter->IsValid())
    {
        index_base::InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        if (!inMemSegment)
        {
            buildingSegIter->MoveToNext();
            continue;
        }

        const index::InMemorySegmentReaderPtr& inMemSegReader = 
            inMemSegment->GetSegmentReader();
        if (!inMemSegReader)
        {
            buildingSegIter->MoveToNext();
            continue;
        }
        assert(mPrimaryKeyIndexConfig);
        index::IndexSegmentReaderPtr pkIndexReader = 
            inMemSegReader->GetSingleIndexSegmentReader(
                    mPrimaryKeyIndexConfig->GetIndexName());
        InMemPrimaryKeySegmentReaderPtr inMemPKSegReader = DYNAMIC_POINTER_CAST(
                InMemPrimaryKeySegmentReaderTyped<Key>, pkIndexReader);
        if (!mBuildingIndexReader)
        {
            mBuildingIndexReader.reset(new PrimaryKeyBuildingIndexReader<Key>());
        }
        mBuildingIndexReader->AddSegmentReader(
                buildingSegIter->GetBaseDocId(), inMemPKSegReader);
        IE_LOG(INFO, "Add In-Memory SegmentReader for segment [%d], by pk index [%s]",
               buildingSegIter->GetSegmentId(), mPrimaryKeyIndexConfig->GetIndexName().c_str());
        buildingSegIter->MoveToNext();
    }
}

template<typename Key>
inline OnDiskOrderedPrimaryKeyIterator<Key> 
PrimaryKeyIndexReaderTyped<Key>::CreateOnDiskOrderedIterator() const
{
    OnDiskOrderedPrimaryKeyIterator<Key> iterator(mPrimaryKeyIndexConfig);
    iterator.Init(mSegmentList);
    return iterator;
}

// other type wiil cause compile error, 
// can not specialization template method of template class
template<typename K, typename T> struct EnableType;// disable
template<typename K> struct EnableType<K, int8_t> { typedef void Enable; };
template<typename K> struct EnableType<K, uint8_t> { typedef void Enable; };
template<typename K> struct EnableType<K, int16_t> { typedef void Enable; };
template<typename K> struct EnableType<K, uint16_t> { typedef void Enable; };
template<typename K> struct EnableType<K, int32_t> { typedef void Enable; };
template<typename K> struct EnableType<K, uint32_t> { typedef void Enable; };
template<typename K> struct EnableType<K, int64_t> { typedef void Enable; };
template<typename K> struct EnableType<K, uint64_t> { typedef void Enable; };


template<> template<typename T>
inline docid_t PrimaryKeyIndexReaderTyped<uint64_t>::LookupWithType(
        const T& key) const
{
    if (likely(mIsNumberHash))
    {
        return Lookup((uint64_t)key);
    }
    std::string strKey = autil::StringUtil::toString(key);
    return Lookup(strKey);
}

template<> template<typename T>
inline docid_t PrimaryKeyIndexReaderTyped<autil::uint128_t>::LookupWithType(
        const T& key) const
{
    std::string strKey = autil::StringUtil::toString(key);
    return Lookup(strKey);
}

template<typename Key>
inline docid_t PrimaryKeyIndexReaderTyped<Key>::LookupWithType(
        const std::string& key) const
{
    Key hashKey;
    if (!Hash(key, hashKey))
    {
        return INVALID_DOCID;
    }
    return Lookup(hashKey);
}

template<typename Key>
size_t  PrimaryKeyIndexReaderTyped<Key>::EstimateLoadSize(
        const index_base::PartitionDataPtr& partitionData,
        const config::IndexConfigPtr& indexConfig,
        const index_base::Version& lastLoadVersion)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexConfig);
    assert(primaryKeyIndexConfig);
    PrimaryKeyLoadStrategyPtr strategy = 
        PrimaryKeyLoadStrategyCreator::CreatePrimaryKeyLoadStrategy(
                primaryKeyIndexConfig);
    PrimaryKeyLoadPlanVector plans;
    strategy->CreatePrimaryKeyLoadPlans(partitionData, plans);
    size_t totalSize = 0;
    for (size_t i = 0; i < plans.size(); i++)
    {
        totalSize += PrimaryKeyLoader<Key>::EstimateLoadSize(
                plans[i], primaryKeyIndexConfig, lastLoadVersion);
    }

    if (primaryKeyIndexConfig->HasPrimaryKeyAttribute())
    {
        AttributeReaderPtr attrReader(new PKAttributeReaderType(
                        primaryKeyIndexConfig->GetIndexName()));
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig());
        attrConfig->Init(primaryKeyIndexConfig->GetFieldConfig());
        totalSize += attrReader->EstimateLoadSize(
                partitionData, attrConfig, lastLoadVersion);
    }
    return totalSize;
}

template<typename Key>
bool PrimaryKeyIndexReaderTyped<Key>::LookupAll(const std::string& pkStr,
        std::vector<std::pair<docid_t, bool>>& docidPairVec) const
{
    Key hashKey;
    if (!Hash(pkStr, hashKey))
    {
        return false;
    }

    for (size_t i = 0; i < mSegmentList.size(); i++)
    {
        docid_t docId = LookupOneSegment(hashKey, mSegmentList[i].first,
                mSegmentList[i].second);
        if (docId != INVALID_DOCID)
        {
            bool isDeleted = mDeletionMapReader && mDeletionMapReader->IsDeleted(docId);
            docidPairVec.push_back(std::make_pair(docId, isDeleted));
        }
    }

    docid_t docId = LookupInMemorySegment(hashKey);
    if (docId != INVALID_DOCID)
    {
        bool isDeleted = mDeletionMapReader && mDeletionMapReader->IsDeleted(docId);
        docidPairVec.push_back(std::make_pair(docId, isDeleted));
    }
    return true;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_READER_TYPED_H
