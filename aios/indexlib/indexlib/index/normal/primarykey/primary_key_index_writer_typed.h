#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_WRITER_TYPED_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_WRITER_TYPED_H

#include <tr1/memory>
#include <autil/HashAlgorithm.h>
#include <autil/StringUtil.h>
#include <autil/LongHashValue.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/primarykey/primary_key_index_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/primary_key_index_dumper.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyIndexWriterTyped : public PrimaryKeyIndexWriter
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::tr1::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef SingleValueAttributeWriter<Key> PKAttributeWriterType;
    typedef std::tr1::shared_ptr<PKAttributeWriterType> PKAttributeWriterTypePtr;
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

public:
    PrimaryKeyIndexWriterTyped()
        : mDocCount(0)
    {        
    }
    
    ~PrimaryKeyIndexWriterTyped() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;

    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override {}
    void Dump(const file_system::DirectoryPtr& dir,
              autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;
    bool CheckPrimaryKeyStr(const std::string& str) const override;
public:
    PKAttributeWriterTypePtr GetPKAttributeWriter() {return mPKAttributeWriter;}
    const HashMapTypedPtr& GetHashMap() const {return mHashMap;}

public:
    // for test
    void SetHashMap(HashMapTypedPtr hashMap){ mHashMap = hashMap; }
    void SetIndexConfig(const config::IndexConfigPtr& indexConfig);
    void SetPKAttributeWriter(const PKAttributeWriterTypePtr& pkAttrWriter)
    { mPKAttributeWriter = pkAttrWriter; }
    void DumpHashMap(const file_system::FileWriterPtr& fileWriter, 
                     const HashMapTypedPtr& hashMap,
                     autil::mem_pool::PoolBase* dumpPool);
private:
    void UpdateBuildResourceMetrics() override;
    uint32_t GetDistinctTermCount() const override;

private:
    util::KeyHasherPtr mHashFunc;
    HashMapTypedPtr mHashMap;
    PKAttributeWriterTypePtr mPKAttributeWriter;
    docid_t mDocCount;

private:
    friend class PrimaryKeyTypedTest;

private:
    IE_LOG_DECLARE();
};

typedef PrimaryKeyIndexWriterTyped<uint64_t> UInt64PrimaryKeyIndexWriterTyped;
typedef std::tr1::shared_ptr<UInt64PrimaryKeyIndexWriterTyped> UInt64PrimaryKeyIndexWriterTypedPtr;

typedef PrimaryKeyIndexWriterTyped<autil::uint128_t> UInt128PrimaryKeyIndexWriterTyped;
typedef std::tr1::shared_ptr<UInt128PrimaryKeyIndexWriterTyped> UInt128PrimaryKeyIndexWriterTypedPtr;

////////////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyIndexWriterTyped);
template<typename Key>
size_t PrimaryKeyIndexWriterTyped<Key>::EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    return 0;
}

template<typename Key>
inline bool PrimaryKeyIndexWriterTyped<Key>::CheckPrimaryKeyStr(
        const std::string& str) const
{
    IndexType indexType = mIndexConfig->GetIndexType();
    return mHashFunc->IsOriginalKeyValid(
            str.c_str(), str.size(), indexType == it_primarykey64);
}

template<typename Key>
void PrimaryKeyIndexWriterTyped<Key>::EndDocument(
        const document::IndexDocument& indexDocument)
{
    const std::string& keyStr = indexDocument.GetPrimaryKey();
    Key primaryKey;
    assert(mHashFunc);
    mHashFunc->GetHashKey(keyStr.c_str(), keyStr.size(), primaryKey);

    docid_t docId = indexDocument.GetDocId();
    //TODO: decide by segment info
    mDocCount++;

    if (mPKAttributeWriter)
    {
        mPKAttributeWriter->AddField(docId, primaryKey);
    }

    mHashMap->FindAndInsert(primaryKey, docId);
    UpdateBuildResourceMetrics();    
}

template<typename Key>
uint32_t PrimaryKeyIndexWriterTyped<Key>::GetDistinctTermCount() const
{
    if(mHashMap)
    {
        return mHashMap->Size();
    }
    return 0;
}

template<typename Key>
void PrimaryKeyIndexWriterTyped<Key>::Dump(
        const file_system::DirectoryPtr& directory,
        autil::mem_pool::PoolBase* dumpPool)
{
    file_system::DirectoryPtr indexDirectory = 
        directory->MakeDirectory(mIndexConfig->GetIndexName());
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(
            PRIMARY_KEY_DATA_FILE_NAME);
    DumpHashMap(fileWriter, mHashMap, dumpPool);
    fileWriter->Close();
    
    if (mPKAttributeWriter)
    {
        const config::AttributeConfigPtr& attrConfig = mPKAttributeWriter->GetAttrConfig();
        
        std::string pkAttributeDirName = 
            std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + attrConfig->GetAttrName();
        file_system::DirectoryPtr pkAttributeDirectory = 
            indexDirectory->MakeDirectory(pkAttributeDirName);
        mPKAttributeWriter->DumpData(pkAttributeDirectory);
    }
    UpdateBuildResourceMetrics();
}

template <typename Key>
index::IndexSegmentReaderPtr PrimaryKeyIndexWriterTyped<Key>::CreateInMemReader()
{
    AttributeSegmentReaderPtr attrSegmentReaderPtr;
    if (mPKAttributeWriter)
    {
        attrSegmentReaderPtr = mPKAttributeWriter->CreateInMemReader();
    }
    return index::IndexSegmentReaderPtr(
            new InMemPrimaryKeySegmentReaderTyped<Key>(mHashMap, 
                    attrSegmentReaderPtr));
}

template <typename Key>
void PrimaryKeyIndexWriterTyped<Key>::Init(
        const config::IndexConfigPtr& indexConfig,
        util::BuildResourceMetrics* buildResourceMetrics,
        const index_base::PartitionSegmentIteratorPtr& segIter)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexConfig);
    if (indexConfig && !primaryKeyIndexConfig)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "indexConfig must be PrimaryKeyIndexConfig");
    }
    IndexWriter::Init(indexConfig, buildResourceMetrics, segIter);
    config::FieldConfigPtr fieldConfig = primaryKeyIndexConfig->GetFieldConfig();
    mHashFunc.reset(util::KeyHasherFactory::CreatePrimaryKeyHasher(
                    fieldConfig->GetFieldType(),
                    primaryKeyIndexConfig->GetPrimaryKeyHashType()));

    if (primaryKeyIndexConfig->HasPrimaryKeyAttribute())
    {
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig(
                                              config::AttributeConfig::ct_pk));
        attrConfig->Init(fieldConfig);

        mPKAttributeWriter.reset(new PKAttributeWriterType(attrConfig));
        mPKAttributeWriter->Init(FSWriterParamDeciderPtr(new DefaultFSWriterParamDecider),
                buildResourceMetrics);
    }

    if (mByteSlicePool.get() != NULL)
    {
        mHashMap.reset(new HashMapTyped(mByteSlicePool.get(), HASHMAP_INIT_SIZE));
    }
    else
    {
        mHashMap.reset(new HashMapTyped(HASHMAP_INIT_SIZE));
    }
    mDocCount = 0;
    UpdateBuildResourceMetrics(); 
}

template<typename Key>
void PrimaryKeyIndexWriterTyped<Key>::SetIndexConfig(
        const config::IndexConfigPtr& indexConfig)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        std::tr1::dynamic_pointer_cast<config::PrimaryKeyIndexConfig>(indexConfig);
    if (indexConfig && !primaryKeyIndexConfig)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, 
                     "indexConfig must be PrimaryKeyIndexConfig");
    }
    mIndexConfig = indexConfig;
}

template <typename Key>
void PrimaryKeyIndexWriterTyped<Key>::DumpHashMap(
        const file_system::FileWriterPtr& file,
        const HashMapTypedPtr& hashMap,
        autil::mem_pool::PoolBase* dumpPool)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig = 
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, mIndexConfig);

    PrimaryKeyIndexDumper<Key> pkDumper(primaryKeyIndexConfig, dumpPool);
    pkDumper.DumpHashMap(hashMap, mDocCount, file);
}

template <typename Key>
void PrimaryKeyIndexWriterTyped<Key>::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode)
    {
        return;
    }    
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        std::tr1::static_pointer_cast<config::PrimaryKeyIndexConfig>(mIndexConfig);

    int64_t poolSize = mByteSlicePool->getUsedBytes() + mSimplePool.getUsedBytes();
    int64_t dumpTempBufferSize =
        PrimaryKeyIndexDumper<Key>::EstimateDumpTempMemoryUse(
                primaryKeyIndexConfig, mDocCount);
    
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    // pk dump process: 1. dump in-mem data to buffer  2. dump buffer to file
    // so, dump_file_size == dump_buffer_size
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpTempBufferSize); 
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_WRITER_H
