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

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentIterator.h"
#include "indexlib/index/kkv/building/SKeyWriter.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDataDumperFactory.h"
#include "indexlib/index/kkv/dump/KKVDumpPhrase.h"
#include "indexlib/index/kkv/dump/KKVFileWriterOptionHelper.h"
#include "indexlib/index/kkv/dump/PKeyDumper.h"
#include "indexlib/index/kkv/dump/SKeyDumperBase.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableIteratorBase.h"
namespace indexlibv2::index {

template <typename SKeyType>
class KKVIndexDumper
{
public:
    using PKeyTable = const PrefixKeyTableBase<SKeyListInfo>;

public:
    KKVIndexDumper(const std::shared_ptr<PKeyTable>& pkeyTable, const std::shared_ptr<SKeyWriter<SKeyType>>& skeyWriter,
                   const std::shared_ptr<KKVValueWriter>& valueWriter,
                   const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, bool isOnline)
        : _pkeyTable(pkeyTable)
        , _skeyWriter(skeyWriter)
        , _valueWriter(valueWriter)
        , _kkvIndexConfig(kkvIndexConfig)
        , _isOnline(isOnline)
    {
    }

public:
    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, autil::mem_pool::PoolBase* pool);
    size_t EstimateDumpFileSize(bool storeTs, double skeyCompressRatio, double valueCompressRatio) const;
    size_t EstimateDumpTmpMemUse(size_t maxValueLen);

private:
    Status DoDump(std::unique_ptr<KKVDataDumperBase>& kkvDataDumper, std::shared_ptr<PKeyTable::Iterator>& pkeyIterator,
                  autil::mem_pool::PoolBase* pool);
    Status DumpSinglePKeyData(std::unique_ptr<KKVDataDumperBase>& kkvDataDumper, uint64_t pkey,
                              const SKeyListInfo& listInfo);

private:
    std::shared_ptr<PKeyTable> _pkeyTable;
    std::shared_ptr<SKeyWriter<SKeyType>> _skeyWriter;
    std::shared_ptr<KKVValueWriter> _valueWriter;
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    bool _isOnline;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVIndexDumper, SKeyType);

//////////////////////////////////////////////////////////////

template <typename SKeyType>
Status KKVIndexDumper<SKeyType>::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                      autil::mem_pool::PoolBase* pool)
{
    AUTIL_LOG(INFO, "Begin dump kkv index");

    auto phrase = _isOnline ? KKVDumpPhrase::RT_BUILD_DUMP : KKVDumpPhrase::BUILD_DUMP;
    // TODO(xinfei.sxf) fix storeTs to false
    auto pkeyIterator = _pkeyTable->CreateIterator();
    auto skeyOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetSkeyParam());
    auto valueOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetValueParam());

    auto kkvDataDumper = KKVDataDumperFactory::Create<SKeyType>(_kkvIndexConfig, /*storeTs*/ true, phrase);
    RETURN_STATUS_DIRECTLY_IF_ERROR(kkvDataDumper->Init(directory, skeyOption, valueOption, pkeyIterator->Size()));
    RETURN_STATUS_DIRECTLY_IF_ERROR(DoDump(kkvDataDumper, pkeyIterator, pool));
    RETURN_STATUS_DIRECTLY_IF_ERROR(kkvDataDumper->Close());

    AUTIL_LOG(INFO,
              "End dump kkv index: total pkey count[%lu], total skey count[%lu], max skey count[%u] in 1 pkey, "
              "max value length[%lu]",
              kkvDataDumper->GetPKeyCount(), kkvDataDumper->GetTotalSKeyCount(), kkvDataDumper->GetMaxSKeyCount(),
              kkvDataDumper->GetMaxValueLen());
    return Status::OK();
}

template <typename SKeyType>
Status KKVIndexDumper<SKeyType>::DoDump(std::unique_ptr<KKVDataDumperBase>& kkvDataDumper,
                                        std::shared_ptr<PKeyTable::Iterator>& pkeyIterator,
                                        autil::mem_pool::PoolBase* pool)
{
    if (!_isOnline) {
        pkeyIterator->SortByKey();
        for (; pkeyIterator->IsValid(); pkeyIterator->MoveToNext()) {
            uint64_t pkey = 0;
            SKeyListInfo listInfo;
            pkeyIterator->Get(pkey, listInfo);
            RETURN_STATUS_DIRECTLY_IF_ERROR(DumpSinglePKeyData(kkvDataDumper, pkey, listInfo));
        }
        return Status::OK();
    }

    using Node = std::pair<uint64_t, SKeyListInfo>;

    autil::mem_pool::pool_allocator<Node> allocator(pool);
    std::vector<Node, decltype(allocator)> nodes(allocator);
    nodes.reserve(pkeyIterator->Size());
    for (; pkeyIterator->IsValid(); pkeyIterator->MoveToNext()) {
        uint64_t pkey = 0;
        SKeyListInfo listInfo;
        pkeyIterator->Get(pkey, listInfo);
        nodes.emplace_back(pkey, listInfo);
    }
    std::sort(nodes.begin(), nodes.end(), [](const Node& lhs, Node& rhs) { return lhs.first < rhs.first; });
    for (const auto& node : nodes) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(DumpSinglePKeyData(kkvDataDumper, node.first, node.second));
    }

    return Status::OK();
}

template <typename SKeyType>
Status KKVIndexDumper<SKeyType>::DumpSinglePKeyData(std::unique_ptr<KKVDataDumperBase>& kkvDataDumper, uint64_t pkey,
                                                    const SKeyListInfo& listInfo)
{
    KKVBuildingSegmentIterator<SKeyType> iter(_skeyWriter, listInfo, _valueWriter, nullptr, _kkvIndexConfig.get());

    if (iter.HasPKeyDeleted()) {
        KKVDoc doc;
        doc.timestamp = iter.GetPKeyDeletedTs();
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            kkvDataDumper->Dump(pkey, /*isDeletedPkey*/ true, /*isLastNode*/ !iter.IsValid(), doc));
    }

    while (iter.IsValid()) {
        KKVDoc doc;
        doc.skey = iter.GetCurrentSkey();
        doc.skeyDeleted = iter.CurrentSkeyDeleted();
        doc.timestamp = iter.GetCurrentTs();
        doc.expireTime = iter.GetCurrentExpireTime();
        if (!doc.skeyDeleted) {
            iter.GetCurrentValue(doc.value);
        }
        iter.MoveToNext();
        RETURN_STATUS_DIRECTLY_IF_ERROR(kkvDataDumper->Dump(pkey, false, !iter.IsValid(), doc));
    }
    return Status::OK();
}

template <typename SKeyType>
size_t KKVIndexDumper<SKeyType>::EstimateDumpFileSize(bool storeTs, double skeyCompressRatio,
                                                      double valueCompressRatio) const
{
    assert(_pkeyTable);
    size_t pkeySize = PKeyDumper::GetTotalDumpSize(_pkeyTable);

    assert(_skeyWriter);
    size_t skeySize = SKeyDumperBase::GetTotalDumpSize<SKeyType>(_skeyWriter->GetTotalSkeyCount(), storeTs,
                                                                 _kkvIndexConfig->StoreExpireTime());

    size_t valueSize = 0;
    if (_valueWriter != nullptr) {
        size_t valueBytes = _valueWriter->GetUsedBytes();
        static constexpr size_t MAX_ALIGN_PADDING_SIZE = (1 << ON_DISK_VALUE_CHUNK_ALIGN_BIT) - 1;
        size_t estimateChunkCount = (valueBytes + KKV_CHUNK_SIZE_THRESHOLD - 1) / KKV_CHUNK_SIZE_THRESHOLD;
        valueSize = valueBytes + estimateChunkCount * (sizeof(ChunkMeta) + MAX_ALIGN_PADDING_SIZE);
    }
    return pkeySize + skeySize * skeyCompressRatio + valueSize * valueCompressRatio;
}

template <typename SKeyType>
size_t KKVIndexDumper<SKeyType>::EstimateDumpTmpMemUse(size_t maxValueLen)
{
    size_t ret = KKV_CHUNK_SIZE_THRESHOLD * 2 + maxValueLen;
    if (_isOnline) {
        ret += _pkeyTable->Size() * sizeof(std::pair<uint64_t, SKeyListInfo>);
    }
    return ret;
}

} // namespace indexlibv2::index
