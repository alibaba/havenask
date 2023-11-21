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

#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/index/normal/attribute/accessor/primary_key_attribute_merger.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/framework/index_merger_creator.h"
#include "indexlib/index/normal/primarykey/block_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/primary_key_merge_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_writer_typed.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/index/util/merger_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyIndexMergerTyped : public IndexMerger
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef PrimaryKeyAttributeMerger<Key> PKAttributeMergerTyped;
    typedef std::shared_ptr<InMemPrimaryKeySegmentReaderTyped<Key>> InMemPrimaryKeySegmentReaderTypedPtr;
    typedef typename PrimaryKeyFormatter<Key>::PKPair PKPair;
    typedef OnDiskOrderedPrimaryKeyIterator<Key> PrimaryKeyIterator;
    typedef OnDiskOrderedPrimaryKeyIterator<Key> KeyOrderedPrimaryKeyIterator;
    typedef std::shared_ptr<OrderedPrimaryKeyIterator<Key>> OrderedPrimaryKeyIteratorPtr;
    typedef std::shared_ptr<PrimaryKeyFormatter<Key>> PrimaryKeyFormatterPtr;

    struct OutputData {
        file_system::FileWriterPtr fileWriter;
    };

public:
    static std::string Identifier()
    {
        std::string ident = "index.merger.parimarykey";
        if (typeid(Key) == typeid(int64_t)) {
            ident += "64";
        } else {
            ident += "128";
        }
        return ident;
    }
    std::string GetIdentifier() const override { return Identifier(); }

    class Creator : public IndexMergerCreator
    {
    public:
        InvertedIndexType GetInvertedIndexType() const
        {
            if (typeid(Key) == typeid(uint64_t)) {
                return it_primarykey64;
            } else if (typeid(Key) == typeid(autil::uint128_t)) {
                return it_primarykey128;
            } else {
                assert(false);
                return it_unknown;
            }
        }
        IndexMerger* Create() const { return (new PrimaryKeyIndexMergerTyped<Key>()); }
    };

public:
    PrimaryKeyIndexMergerTyped() {}
    ~PrimaryKeyIndexMergerTyped() {}

public:
    // void Merge(const index_base::SegmentMergeInfos& segMergeInfos,
    //            const std::vector<index::IndexMergerResource>& resources);

    // void SortByWeightMerge(
    //         const index_base::SegmentMergeInfos& segMergeInfos,
    //         const std::vector<index::IndexMergerResource>& resources);

    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

private:
    void InnerMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool sortByWeight);

    void MergePkData(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                     const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool sortByWeight);

    void MergePkAttribute(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                          const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool sortByWeight);

    PrimaryKeyFormatterPtr CreatePkFormatter(const config::PrimaryKeyIndexConfigPtr& pkConfig) const;

    OrderedPrimaryKeyIteratorPtr CreatePkIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                                                  const ReclaimMapPtr& reclaimMap,
                                                  const std::vector<index_base::SegmentData>& segmentDatas) const;

    file_system::FileWriterPtr CreateFileWriter(const file_system::DirectoryPtr& directory,
                                                const config::PrimaryKeyIndexConfigPtr& pkConfig) const;

private:
    IE_LOG_DECLARE();
};

typedef PrimaryKeyIndexMergerTyped<uint64_t> PrimaryKey64IndexMerger;
typedef PrimaryKeyIndexMergerTyped<autil::uint128_t> PrimaryKey128IndexMerger;

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyIndexMergerTyped);

template <typename Key>
void PrimaryKeyIndexMergerTyped<Key>::InnerMerge(const MergerResource& resource,
                                                 const index_base::SegmentMergeInfos& segMergeInfos,
                                                 const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                 bool sortByWeight)
{
    MergePkData(resource, segMergeInfos, outputSegMergeInfos, sortByWeight);
    MergePkAttribute(resource, segMergeInfos, outputSegMergeInfos, sortByWeight);
}

template <typename Key>
void PrimaryKeyIndexMergerTyped<Key>::MergePkData(const MergerResource& resource,
                                                  const index_base::SegmentMergeInfos& segMergeInfos,
                                                  const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                  bool sortByWeight)
{
    IE_LOG(INFO, "merge primary key data begin");
    const SegmentDirectoryBasePtr& segDir = GetSegmentDirectory();
    const index_base::PartitionDataPtr& partData = segDir->GetPartitionData();
    std::vector<index_base::SegmentData> segmentDatas;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        segmentDatas.push_back(partData->GetSegmentData(segMergeInfos[i].segmentId));
    }

    config::PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, GetIndexConfig());

    OrderedPrimaryKeyIteratorPtr pkIter = CreatePkIterator(pkConfig, resource.reclaimMap, segmentDatas);
    PrimaryKeyFormatterPtr pkFormatter = PrimaryKeyFormatterCreator<Key>::CreatePKFormatterByIndexType(pkConfig);

    SegmentOutputMapper<PKMergeOutputData> outputMapper;
    outputMapper.Init(resource.reclaimMap, outputSegMergeInfos,
                      [this, &pkConfig](const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx) {
                          PKMergeOutputData output;
                          output.outputIdx = outputIdx;
                          output.targetSegmentIndex = outputInfo.targetSegmentIndex;
                          output.fileWriter = CreateFileWriter(outputInfo.directory, pkConfig);
                          return output;
                      });

    assert(pkFormatter);
    pkFormatter->Format(pkIter, outputMapper);

    for (const auto& output : outputMapper.GetOutputs()) {
        output.fileWriter->Close().GetOrThrow();
    }

    IE_LOG(INFO, "merge primary key data end");
}

template <typename Key>
void PrimaryKeyIndexMergerTyped<Key>::MergePkAttribute(const MergerResource& resource,
                                                       const index_base::SegmentMergeInfos& segMergeInfos,
                                                       const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                                       bool sortByWeight)
{
    IE_LOG(INFO, "merge primary key attribute begin");
    config::PrimaryKeyIndexConfigPtr primaryKeyConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, GetIndexConfig());

    if (primaryKeyConfig->HasPrimaryKeyAttribute()) {
        config::AttributeConfigPtr attrConfig(new config::AttributeConfig());
        attrConfig->Init(primaryKeyConfig->GetFieldConfig());

        AttributeMergerPtr pkAttrMerger(new PKAttributeMergerTyped(primaryKeyConfig->GetIndexName()));
        pkAttrMerger->Init(attrConfig, mMergeHint, mTaskResources);
        pkAttrMerger->BeginMerge(GetSegmentDirectory());
        if (sortByWeight) {
            pkAttrMerger->SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
        } else {
            pkAttrMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
        }
    }
    IE_LOG(INFO, "merge primary key attribute end");
}

// template <typename Key>
// void PrimaryKeyIndexMergerTyped<Key>::SortByWeightMerge(
//     const index_base::SegmentMergeInfos& segMergeInfos,
//     const std::vector<index::IndexMergerResource>& resources)
// {
//    // InnerMerge(resources[0].directory, segMergeInfos, resources[0].reclaimMap, true);
//     // InnerMerge(dir, segMergeInfos, resource.reclaimMap, true);
//     MergerResource resource;
//     resource.reclaimMap = resources[0].reclaimMap;
//     resource.targetSegmentCount = resources.size();
//     index_base::OutputSegmentMergeInfos outputInfos;
//     for (size_t i = 0; i < resources.size(); ++i)
//     {
//         outputInfos.emplace_back();
//         outputInfos.back().targetSegmentIndex = i;
//         outputInfos.back().directory = resources[i].directory;
//     }
//     SortByWeightMerge(resource, segMergeInfos, outputInfos);
// }

// template <typename Key>
// void PrimaryKeyIndexMergerTyped<Key>::Merge(
//     const index_base::SegmentMergeInfos& segMergeInfos,
//     const std::vector<index::IndexMergerResource>& resources)
// {
//     // InnerMerge(resources[0].directory, segMergeInfos, resources[0].reclaimMap, false);
//     // InnerMerge(dir, segMergeInfos, resource.reclaimMap, false);
//     MergerResource resource;
//     resource.reclaimMap = resources[0].reclaimMap;
//     resource.targetSegmentCount = resources.size();
//     index_base::OutputSegmentMergeInfos outputInfos;
//     for (size_t i = 0; i < resources.size(); ++i)
//     {
//         outputInfos.emplace_back();
//         outputInfos.back().targetSegmentIndex = i;
//         outputInfos.back().directory = resources[i].directory;
//     }
//     Merge(resource, segMergeInfos, outputInfos);
//}

template <typename Key>
void PrimaryKeyIndexMergerTyped<Key>::SortByWeightMerge(const index::MergerResource& resource,
                                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)

{
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos, true);
}

template <typename Key>
void PrimaryKeyIndexMergerTyped<Key>::Merge(const index::MergerResource& resource,
                                            const index_base::SegmentMergeInfos& segMergeInfos,
                                            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)

{
    InnerMerge(resource, segMergeInfos, outputSegMergeInfos, false);
}

template <typename Key>
int64_t PrimaryKeyIndexMergerTyped<Key>::EstimateMemoryUse(
    const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos, const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
    bool isSortedMerge) const
{
    int64_t size = sizeof(*this);
    size += file_system::WriterOption::DEFAULT_BUFFER_SIZE * outputSegMergeInfos.size(); // write buffer

    config::PrimaryKeyIndexConfigPtr primaryKeyConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, GetIndexConfig());
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        totalDocCount += segMergeInfos[i].segmentInfo.docCount;
    }
    switch (primaryKeyConfig->GetPrimaryKeyIndexType()) {
    case pk_hash_table:
        size += totalDocCount * PrimaryKeyHashTable<Key>::EstimateMemoryCostPerDoc();
        break;
    case pk_sort_array:
        break;
    case pk_block_array:
        break;
    }
    // todo: estimate pk attribute memory use
    return size;
}

template <typename Key>
typename PrimaryKeyIndexMergerTyped<Key>::OrderedPrimaryKeyIteratorPtr
PrimaryKeyIndexMergerTyped<Key>::CreatePkIterator(const config::PrimaryKeyIndexConfigPtr& pkConfig,
                                                  const ReclaimMapPtr& reclaimMap,
                                                  const std::vector<index_base::SegmentData>& segmentDatas) const
{
    OrderedPrimaryKeyIteratorPtr mergeIterator(new PrimaryKeyMergeIterator<Key>(pkConfig, reclaimMap));
    mergeIterator->Init(segmentDatas);
    return mergeIterator;
}

template <typename Key>
file_system::FileWriterPtr
PrimaryKeyIndexMergerTyped<Key>::CreateFileWriter(const file_system::DirectoryPtr& directory,
                                                  const config::PrimaryKeyIndexConfigPtr& pkConfig) const
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    directory->RemoveDirectory(pkConfig->GetIndexName(), removeOption);
    file_system::DirectoryPtr pkDirectory = directory->MakeDirectory(pkConfig->GetIndexName());
    return pkDirectory->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME);
}
}} // namespace indexlib::index
