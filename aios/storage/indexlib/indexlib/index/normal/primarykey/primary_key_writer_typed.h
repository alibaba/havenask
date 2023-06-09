/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/primary_key_index_dumper.h"
#include "indexlib/index/normal/primarykey/primary_key_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyWriterTyped : public PrimaryKeyWriter
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef SingleValueAttributeWriter<Key> PKAttributeWriterType;
    typedef std::shared_ptr<PKAttributeWriterType> PKAttributeWriterTypePtr;
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;

public:
    PrimaryKeyWriterTyped() : mDocCount(0) {}
    ~PrimaryKeyWriterTyped() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override {}
    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;
    std::shared_ptr<index::IndexSegmentReader> CreateInMemReader() override;
    bool CheckPrimaryKeyStr(const std::string& str) const override;

private:
    void DumpHashMap(const file_system::FileWriterPtr& fileWriter, const HashMapTypedPtr& hashMap,
                     autil::mem_pool::PoolBase* dumpPool);

    void UpdateBuildResourceMetrics() override;
    uint32_t GetDistinctTermCount() const override;

private:
    FieldType _fieldType;
    PrimaryKeyHashType _primaryKeyHashType;

    util::SimplePool _simplePool;
    HashMapTypedPtr mHashMap;
    PKAttributeWriterTypePtr mPKAttributeWriter;
    docid_t mDocCount;

private:
    friend class PrimaryKeyTypedTest;

private:
    IE_LOG_DECLARE();
};

typedef PrimaryKeyWriterTyped<uint64_t> UInt64PrimaryKeyWriterTyped;
typedef std::shared_ptr<UInt64PrimaryKeyWriterTyped> UInt64PrimaryKeyWriterTypedPtr;

typedef PrimaryKeyWriterTyped<autil::uint128_t> UInt128PrimaryKeyWriterTyped;
typedef std::shared_ptr<UInt128PrimaryKeyWriterTyped> UInt128PrimaryKeyWriterTypedPtr;

////////////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyWriterTyped);
template <typename Key>
size_t PrimaryKeyWriterTyped<Key>::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                      const index_base::PartitionSegmentIteratorPtr& segIter)
{
    return 0;
}

template <typename Key>
inline bool PrimaryKeyWriterTyped<Key>::CheckPrimaryKeyStr(const std::string& str) const
{
    InvertedIndexType indexType = _indexConfig->GetInvertedIndexType();
    return index::KeyHasherWrapper::IsOriginalKeyValid(_fieldType, _primaryKeyHashType, str.c_str(), str.size(),
                                                       indexType == it_primarykey64);
}

template <typename Key>
void PrimaryKeyWriterTyped<Key>::EndDocument(const document::IndexDocument& indexDocument)
{
    const std::string& keyStr = indexDocument.GetPrimaryKey();
    Key primaryKey;
    assert(_indexConfig->GetInvertedIndexType() == it_primarykey64 or
           _indexConfig->GetInvertedIndexType() == it_primarykey128);
    bool ret =
        index::KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, keyStr.c_str(), keyStr.size(), primaryKey);
    assert(ret);
    (void)ret;

    docid_t docId = indexDocument.GetDocId();
    // TODO: decide by segment info
    mDocCount++;
    if (mPKAttributeWriter) {
        mPKAttributeWriter->AddField(docId, primaryKey);
    }
    mHashMap->FindAndInsert(primaryKey, docId);
    UpdateBuildResourceMetrics();
}

template <typename Key>
uint32_t PrimaryKeyWriterTyped<Key>::GetDistinctTermCount() const
{
    if (mHashMap) {
        return mHashMap->Size();
    }
    return 0;
}

template <typename Key>
void PrimaryKeyWriterTyped<Key>::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool)
{
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(_indexConfig->GetIndexName());
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME);
    DumpHashMap(fileWriter, mHashMap, dumpPool);
    fileWriter->Close().GetOrThrow();

    if (mPKAttributeWriter) {
        const config::AttributeConfigPtr& attrConfig = mPKAttributeWriter->GetAttrConfig();

        std::string pkAttributeDirName = std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + attrConfig->GetAttrName();
        file_system::DirectoryPtr pkAttributeDirectory = indexDirectory->MakeDirectory(pkAttributeDirName);
        mPKAttributeWriter->DumpData(pkAttributeDirectory);
    }
    UpdateBuildResourceMetrics();
}

template <typename Key>
std::shared_ptr<index::IndexSegmentReader> PrimaryKeyWriterTyped<Key>::CreateInMemReader()
{
    AttributeSegmentReaderPtr attrSegmentReaderPtr;
    if (mPKAttributeWriter) {
        attrSegmentReaderPtr = mPKAttributeWriter->CreateInMemReader();
    }
    return std::shared_ptr<index::IndexSegmentReader>(
        new InMemPrimaryKeySegmentReaderTyped<Key>(mHashMap, attrSegmentReaderPtr));
}

template <typename Key>
void PrimaryKeyWriterTyped<Key>::Init(const config::IndexConfigPtr& indexConfig,
                                      util::BuildResourceMetrics* buildResourceMetrics)

{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexConfig);
    if (indexConfig && !primaryKeyIndexConfig) {
        INDEXLIB_FATAL_ERROR(BadParameter, "indexConfig must be PrimaryKeyIndexConfig");
    }
    IndexWriter::Init(indexConfig, buildResourceMetrics);
    config::FieldConfigPtr fieldConfig = primaryKeyIndexConfig->GetFieldConfig();
    _fieldType = fieldConfig->GetFieldType();
    _primaryKeyHashType = primaryKeyIndexConfig->GetPrimaryKeyHashType();

    if (primaryKeyIndexConfig->HasPrimaryKeyAttribute()) {
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig(config::AttributeConfig::ct_pk));
        attrConfig->Init(fieldConfig);

        mPKAttributeWriter.reset(new PKAttributeWriterType(attrConfig));
        mPKAttributeWriter->Init(FSWriterParamDeciderPtr(new DefaultFSWriterParamDecider), buildResourceMetrics);
    }

    if (_byteSlicePool.get() != NULL) {
        mHashMap.reset(new HashMapTyped(_byteSlicePool.get(), HASHMAP_INIT_SIZE));
    } else {
        mHashMap.reset(new HashMapTyped(HASHMAP_INIT_SIZE));
    }
    mDocCount = 0;
    UpdateBuildResourceMetrics();
}

template <typename Key>
void PrimaryKeyWriterTyped<Key>::DumpHashMap(const file_system::FileWriterPtr& file, const HashMapTypedPtr& hashMap,
                                             autil::mem_pool::PoolBase* dumpPool)
{
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, _indexConfig);

    PrimaryKeyIndexDumper<Key> pkDumper(primaryKeyIndexConfig, dumpPool);
    pkDumper.DumpHashMap(hashMap, mDocCount, file);
}

template <typename Key>
void PrimaryKeyWriterTyped<Key>::UpdateBuildResourceMetrics()
{
    if (!_buildResourceMetricsNode) {
        return;
    }
    config::PrimaryKeyIndexConfigPtr primaryKeyIndexConfig =
        std::static_pointer_cast<config::PrimaryKeyIndexConfig>(_indexConfig);

    int64_t poolSize = _byteSlicePool->getUsedBytes() + _simplePool.getUsedBytes();
    int64_t dumpTempBufferSize =
        PrimaryKeyIndexDumper<Key>::EstimateDumpTempMemoryUse(primaryKeyIndexConfig, mDocCount);

    _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    // pk dump process: 1. dump in-mem data to buffer  2. dump buffer to file
    // so, dump_file_size == dump_buffer_size
    _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpTempBufferSize);
}

}} // namespace indexlib::index
