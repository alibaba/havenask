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

#include "autil/Defer.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/primary_key/InMemPrimaryKeySegmentReaderTyped.h"
#include "indexlib/index/primary_key/PrimaryKeyAttributeReader.h"
#include "indexlib/index/primary_key/PrimaryKeyBuildingIndexReader.h"
#include "indexlib/index/primary_key/PrimaryKeyDuplicationChecker.h"
#include "indexlib/index/primary_key/PrimaryKeyHashTable.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/primary_key/PrimaryKeyIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyLoadPlan.h"
#include "indexlib/index/primary_key/PrimaryKeyPostingIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/primary_key/PrimaryKeyWriter.h"
#include "indexlib/index/primary_key/SegmentDataAdapter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/primary_key/merger/OnDiskOrderedPrimaryKeyIterator.h"
#include "indexlib/util/FutureExecutor.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2::framework {
struct SegmentMeta;
class Segment;
class TabletData;
} // namespace indexlibv2::framework
namespace indexlibv2::index {
class IIndex;
}

namespace indexlibv2::index {

class SegmentDataAdapter;

template <typename Key, typename DerivedType = void>
class PrimaryKeyReader : public indexlib::index::PrimaryKeyIndexReader, public autil::NoCopyable
{
public:
    using PKPairTyped = index::PKPair<Key, docid64_t>;
    using SegmentPKPairTyped = index::PKPair<Key, docid_t>;
    using Segments = std::vector<framework::Segment*>;

    using HashMapTyped = indexlib::util::HashMap<Key, docid_t>;
    using HashMapTypedPtr = std::shared_ptr<HashMapTyped>;

    using SegmentReader = PrimaryKeyDiskIndexer<Key>;
    using SegmentReaderPtr = std::shared_ptr<SegmentReader>;
    using PrimaryKeySegment = std::pair<docid64_t, SegmentReaderPtr>;
    using PrimaryKeyBuildingReader = indexlib::index::PrimaryKeyBuildingIndexReader<Key>;
    using InMemPrimaryKeySegmentReaderPtr = std::shared_ptr<indexlib::index::InMemPrimaryKeySegmentReaderTyped<Key>>;

public:
    typedef struct {
        PrimaryKeySegment _segmentPair;
        std::vector<segmentid_t> _segmentIds;
    } SegmentReaderInfoType;

    explicit PrimaryKeyReader(framework::MetricsManager* metricsManager)
        : PrimaryKeyIndexReader()
        , _metricsManager(metricsManager)
    {
    }

    ~PrimaryKeyReader() {}

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;

    Status OpenWithoutPKAttribute(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const framework::TabletData* tabletData) override;

    std::shared_ptr<indexlibv2::index::AttributeReader> GetPKAttributeReader() const override
    {
        return _pkAttributeReader;
    }

    size_t EstimateLoadSize(const Segments& allSegments, const std::shared_ptr<config::IIndexConfig>& indexConfig);
    size_t EvaluateCurrentMemUsed() override;

public:
    indexlib::index::Result<indexlib::index::PostingIterator*>
    Lookup(const indexlib::index::Term& term, uint32_t statePoolSize = 1000, PostingType type = pt_default,
           autil::mem_pool::Pool* sessionPool = NULL) override;
    docid64_t Lookup(const autil::StringView& pkStr) const override;
    docid64_t Lookup(const std::string& strKey, future_lite::Executor* executor) const override;
    docid64_t LookupWithHintValues(const autil::uint128_t& pkHash, int32_t hintValues) const override;
    docid64_t LookupWithPKHash(const autil::uint128_t& pkHash, future_lite::Executor* executor) const override;
    bool LookupWithPKHash(const autil::uint128_t& pkHash, segmentid_t specifySegment, docid64_t* docid) const override;
    docid64_t LookupWithDocRange(const autil::uint128_t& pkHash, std::pair<docid_t, docid_t> docRange,
                                 future_lite::Executor* executor) const override;
    bool LookupAll(const std::string& pkStr, std::vector<std::pair<docid64_t, bool>>& docidPairVec) const override;

    docid64_t Lookup(const std::string& strKey) const override { return Lookup(strKey, nullptr); }

    bool CheckDuplication() const override;

public:
    docid64_t Lookup(const Key& key, future_lite::Executor* executor = nullptr) const __ALWAYS_INLINE;
    docid64_t Lookup(const Key& key, docid64_t& lastDocId) const;
    docid64_t Lookup(const std::string& pkStr, docid64_t& lastDocId) const;

    // TODO (远轫) 可能需要用executor加速查询
    std::pair<Status, std::vector<bool>>
    Contains(const std::vector<const document::IIndexFields*>& indexFields) const override;

public:
    static std::string Identifier()
    {
        static const std::string PRIMARY_KEY_DEFAULT_READER_IDENTIFIER("primary_key.typed.reader");
        static const std::string PRIMARY_KEY_UINT64_READER_IDENTIFIER("primary_key.uint64.reader");
        static const std::string PRIMARY_KEY_UINT128_READER_IDENTIFIER("primary_key.uint128.reader");
        if (typeid(Key) == typeid(uint64_t)) {
            return PRIMARY_KEY_UINT64_READER_IDENTIFIER;
        } else if (typeid(Key) == typeid(autil::uint128_t)) {
            return PRIMARY_KEY_UINT128_READER_IDENTIFIER;
        }
        return PRIMARY_KEY_DEFAULT_READER_IDENTIFIER;
    }
    future_lite::Executor* GetBuildExecutor() const override { return _buildExecutor; }
    FieldType GetFieldType() const { return _fieldType; }
    PrimaryKeyHashType GetPrimaryKeyHashType() const { return _primaryKeyHashType; }

public:
    std::unique_ptr<OnDiskOrderedPrimaryKeyIterator<Key>> LEGACY_CreateOnDiskOrderedIterator() const
    {
        auto iterator = std::make_unique<OnDiskOrderedPrimaryKeyIterator<Key>>();
        if (!iterator->Init(_primaryKeyIndexConfig, _segmentDatas)) {
            AUTIL_LOG(ERROR, "OnDiskOrderedPrimaryKeyIterator init failed!");
            return nullptr;
        }
        return iterator;
    }

protected:
    void AddBuildingIndex(segmentid_t segId, const framework::SegmentInfo& segInfo,
                          const std::shared_ptr<const HashMapTyped>& hashMap);

    bool IsDocIdValid(docid64_t docid) const __ALWAYS_INLINE;
    docid64_t LookupInMemorySegment(const Key& hashKey) const;
    docid64_t LookupOnDiskSegments(const Key& hashKey, docid64_t& lastDocId) const __ALWAYS_INLINE;
    docid64_t LookupOnDiskSegments(const Key& hashKey, future_lite::Executor* executor) const __ALWAYS_INLINE;
    future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>
    LookupOnDiskSegmentsAsync(const Key& hashKey, future_lite::Executor* executor) const noexcept;
    future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>
    LookupOneSegmentAsync(const Key& hashKey, docid64_t baseDocid,
                          const std::shared_ptr<PrimaryKeyDiskIndexer<Key>>& segReader,
                          future_lite::Executor* executor) const noexcept;
    indexlib::index::Result<docid64_t>
    LookupOneSegment(const Key& hashKey, docid64_t baseDocid,
                     const std::shared_ptr<PrimaryKeyDiskIndexer<Key>>& segReader) const noexcept __ALWAYS_INLINE;
    bool InnerLookupWithPKHash(const Key& hashKey, segmentid_t specifySegment, docid64_t* docid) const;
    docid64_t InnerLookupWithHintValues(const Key& pkHash, int32_t hintValues) const;
    docid64_t InnerLookupWithDocRange(const Key& hashKey, const std::pair<docid32_t, docid32_t> docRange,
                                      future_lite::Executor* executor) const;
    bool GetDocIdRanges(int32_t hintValues, DocIdRangeVector& docIdRanges) const;

    void CreateLoadPlans(std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
                         std::vector<std::unique_ptr<PrimaryKeyLoadPlan>>& plans,
                         const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

    [[nodiscard]] bool CanDirectLoad(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig) const;
    [[nodiscard]] bool NeedCombineSegments() const;
    [[nodiscard]] std::shared_ptr<indexlib::file_system::FileWriter> CreateTargetSliceFile(PrimaryKeyLoadPlan* plan,
                                                                                           const std::string& fileName);
    [[nodiscard]] inline bool DoOpen(std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
                                     bool forceReverseLookup);
    inline void InnerInitbuilding();
    inline void InnerInit();
    void AppendBuildingIndexes(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               framework::TabletData::Slice& slice);

    [[nodiscard]] inline size_t
    DoEstimateLoadSize(std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
                       const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                       bool isValidVersion);
    bool DocDeleted(docid64_t docid) const;
    inline bool IsDocDeleted(docid64_t docid) const
    {
        return _deletionMapReader && _deletionMapReader->IsDeleted(docid);
    }

protected:
    // protected for fake
    bool Hash(const std::string& keyStr, Key& key) const
    {
        // TODO(panghai.hj): Figure out how to assert mPrimaryKeyIndexConfig correctly.
        // assert(mPrimaryKeyIndexConfig);
        // assert(mPrimaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey64 or
        //        mPrimaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey128);
        return indexlib::index::KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, keyStr.c_str(),
                                                             keyStr.size(), key);
    }

private:
    //    bool GetLocalDocidInfo(docid64_t gDocId, std::string& segIds, docid64_t& localDocid) const;

private:
    std::shared_ptr<AttributeReader> _pkAttributeReader;

protected:
    FieldType _fieldType = FieldType::ft_unknown;
    PrimaryKeyHashType _primaryKeyHashType = pk_default_hash;

    bool _needLookupReverse = false;
    docid64_t _baseDocid = 0;

    std::unique_ptr<DeletionMapIndexReader> _deletionMapReader;
    future_lite::Executor* _buildExecutor = nullptr;

    std::shared_ptr<index::PrimaryKeyIndexConfig> _primaryKeyIndexConfig;
    std::unique_ptr<PrimaryKeyBuildingReader> _buildingIndexReader;

    std::vector<std::unique_ptr<PrimaryKeyLoadPlan>> _loadPlans;
    std::vector<SegmentReaderInfoType> _segmentReaderList;
    std::vector<SegmentDataAdapter::SegmentDataType> _segmentDatas;
    framework::MetricsManager* _metricsManager;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, PrimaryKeyReader, Key, DerivedType);
typedef PrimaryKeyReader<uint64_t> UInt64PrimaryKeyReader;
typedef std::shared_ptr<UInt64PrimaryKeyReader> UInt64PrimaryKeyReaderPtr;
typedef PrimaryKeyReader<autil::uint128_t> UInt128PrimaryKeyReader;
typedef std::shared_ptr<UInt128PrimaryKeyReader> UInt128PrimaryKeyReaderPtr;

inline bool CompByDocCount(const std::unique_ptr<PrimaryKeyLoadPlan>& leftPlan,
                           const std::unique_ptr<PrimaryKeyLoadPlan>& rightPlan)
{
    if (leftPlan->GetDocCount() != rightPlan->GetDocCount()) {
        return leftPlan->GetDocCount() > rightPlan->GetDocCount();
    }
    return leftPlan->GetBaseDocId() > rightPlan->GetBaseDocId();
}

inline bool CompByBaseDocId(const std::unique_ptr<PrimaryKeyLoadPlan>& leftPlan,
                            const std::unique_ptr<PrimaryKeyLoadPlan>& rightPlan)
{
    return leftPlan->GetBaseDocId() > rightPlan->GetBaseDocId();
}

template <typename Key, typename DerivedType>
inline void
PrimaryKeyReader<Key, DerivedType>::AppendBuildingIndexes(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                          framework::TabletData::Slice& slice)
{
    for (auto& segment : slice) {
        auto index = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName()).second;
        auto indexWriter = std::dynamic_pointer_cast<index::PrimaryKeyWriter<Key>>(index);
        AddBuildingIndex(segment->GetSegmentId(), *(segment->GetSegmentInfo()), indexWriter->GetHashMap());
    }
}

template <typename Key, typename DerivedType>
Status PrimaryKeyReader<Key, DerivedType>::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                const framework::TabletData* tabletData)
{
    auto status = OpenWithoutPKAttribute(indexConfig, tabletData);
    RETURN_IF_STATUS_ERROR(status, "open pk reader failed");

    if (_primaryKeyIndexConfig->HasPrimaryKeyAttribute()) {
        _pkAttributeReader = std::make_shared<PrimaryKeyAttributeReader<Key>>();
        auto status = _pkAttributeReader->Open(indexConfig, tabletData);
        RETURN_IF_STATUS_ERROR(status, "open pk attribute reader failed");
    }

    return Status::OK();
}

template <typename Key, typename DerivedType>
Status
PrimaryKeyReader<Key, DerivedType>::OpenWithoutPKAttribute(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                           const framework::TabletData* tabletData)
{
    Segments segments;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        segments.push_back(iter->get());
    }
    _indexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
    assert(_indexConfig);
    _primaryKeyIndexConfig = std::dynamic_pointer_cast<index::PrimaryKeyIndexConfig>(indexConfig);

    InnerInit();
    SegmentDataAdapter::Transform(segments, _segmentDatas);

    if (!DoOpen(_segmentDatas, false)) {
        return Status::InternalError("primaryKeyReader doOpen failed");
    }

    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_DUMPING);
    AppendBuildingIndexes(indexConfig, slice);
    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    AppendBuildingIndexes(indexConfig, slice);
    auto [status, deletionMapReader] = DeletionMapIndexReader::Create(tabletData);
    RETURN_IF_STATUS_ERROR(status, "open deletion map reader failed");
    _deletionMapReader = std::move(deletionMapReader);
    return Status::OK();
}

template <typename Key, typename DerivedType>
void PrimaryKeyReader<Key, DerivedType>::AddBuildingIndex(segmentid_t segId, const framework::SegmentInfo& segInfo,
                                                          const std::shared_ptr<const HashMapTyped>& hashMap)
{
    if (!_buildingIndexReader) {
        _buildingIndexReader.reset(new indexlib::index::PrimaryKeyBuildingIndexReader<Key>());
    }
    // todo: yijie.zhang, use segId to add segment reader
    _buildingIndexReader->AddSegmentReader(_baseDocid, segId, hashMap);
    _baseDocid += segInfo.docCount;
}

template <typename Key, typename DerivedType>
inline void PrimaryKeyReader<Key, DerivedType>::InnerInit()
{
    assert(_primaryKeyIndexConfig);
    auto fieldConfig = _primaryKeyIndexConfig->GetFieldConfig();
    _isNumberHash = _primaryKeyIndexConfig->GetPrimaryKeyHashType() == pk_number_hash;
    _is128Pk = _primaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey128;
    _fieldType = fieldConfig->GetFieldType();
    _primaryKeyHashType = _primaryKeyIndexConfig->GetPrimaryKeyHashType();
    if (_primaryKeyIndexConfig->IsParallelLookupOnBuild()) {
        _buildExecutor = indexlib::util::FutureExecutor::GetInternalBuildExecutor();
        AUTIL_LOG(INFO, "pk build parallel enable, get executor [%p]", _buildExecutor);
    }
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::DoOpen(std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
                                                       bool forceReverseLookup)
{
    try {
        _primaryKeyIndexConfig->Check();
    } catch (...) {
        AUTIL_LOG(ERROR, "primaryKey index config check failed");
        return false;
    }
    assert(_primaryKeyIndexConfig);
    CreateLoadPlans(segmentDatas, _loadPlans, _primaryKeyIndexConfig);

    _needLookupReverse =
        forceReverseLookup ? true : _primaryKeyIndexConfig->GetPKLoadStrategyParam().NeedLookupReverse();
    if (_needLookupReverse) {
        std::sort(_loadPlans.begin(), _loadPlans.end(), CompByBaseDocId);
    } else {
        std::sort(_loadPlans.begin(), _loadPlans.end(), CompByDocCount);
    }
    for (auto& plan : _loadPlans) {
        std::string fileName(PRIMARY_KEY_DATA_FILE_NAME);
        DiskIndexerParameter indexerParam;
        std::vector<segmentid_t> segments;
        plan->GetSegmentIdList(&segments);
        indexerParam.metricsManager = _metricsManager;
        indexerParam.docCount = plan->GetLastSegmentDocCount();
        auto diskIndexer = std::make_shared<PrimaryKeyDiskIndexer<Key>>(indexerParam);
        _baseDocid += plan->GetDocCount();
        auto [status, directory] = plan->GetPrimaryKeyDirectory(_primaryKeyIndexConfig);
        if (!status.IsOK() || nullptr == directory) {
            AUTIL_LOG(ERROR, "fail to get primary key directory");
            return false;
        }
        auto [st, pkDataDir] = directory->GetDirectory(_primaryKeyIndexConfig->GetIndexName()).StatusWith();
        if (!st.IsOK() || nullptr == pkDataDir) {
            AUTIL_LOG(ERROR, "fail to get primary key data directory");
            return false;
        }

        if (!CanDirectLoad(_primaryKeyIndexConfig) || plan->GetSegmentNum() > 1) {
            fileName = plan->GetTargetSliceFileName();
            auto [readerStatus, sliceFileReader] =
                pkDataDir->CreateFileReader(fileName, indexlib::file_system::FSOT_SLICE).StatusWith();
            std::shared_ptr<indexlib::file_system::FileWriter> fileWriter;
            if (!readerStatus.IsOK() || !sliceFileReader) {
                fileWriter = CreateTargetSliceFile(plan.get(), fileName);
                if (!fileWriter) {
                    AUTIL_LOG(ERROR, "fail to create target slice file: [%s]", fileName.c_str());
                    return false;
                }
            }
            autil::Defer defer([&fileWriter]() {
                if (fileWriter) {
                    fileWriter->Close().GetOrThrow();
                }
            });
            if (!diskIndexer->OpenWithSliceFile(_primaryKeyIndexConfig, pkDataDir, fileName)) {
                AUTIL_LOG(ERROR, "primary key reader open fail [%s]", fileName.c_str());
                return false;
            }
        } else {
            auto status = diskIndexer->Open(_primaryKeyIndexConfig, directory);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "primary key reader open fail [%s]", directory->DebugString().c_str());
                return false;
            }
        }
        AUTIL_LOG(DEBUG, "primary key load plan target [%s]", fileName.c_str());
        _segmentReaderList.push_back({std::make_pair(plan->GetBaseDocId(), diskIndexer), segments});
        if (!plan->SetSegmentIndexer(_primaryKeyIndexConfig, diskIndexer)) {
            return false;
        }
    }

    return true;
}

template <typename Key, typename DerivedType>
bool PrimaryKeyReader<Key, DerivedType>::CanDirectLoad(
    const std::shared_ptr<config::InvertedIndexConfig>& indexConfig) const
{
    using namespace indexlib::config;
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> pkIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);

    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode =
        pkIndexConfig->GetPKLoadStrategyParam().GetPrimaryKeyLoadMode();
    PrimaryKeyIndexType pkIndexType = pkIndexConfig->GetPrimaryKeyIndexType();
    if (loadMode == PrimaryKeyLoadStrategyParam::HASH_TABLE && pkIndexType == pk_hash_table) {
        return true;
    }

    if (loadMode == PrimaryKeyLoadStrategyParam::SORTED_VECTOR && pkIndexType == pk_sort_array) {
        return true;
    }

    if (loadMode == PrimaryKeyLoadStrategyParam::BLOCK_VECTOR && pkIndexType == pk_block_array) {
        return true;
    }
    assert(loadMode == PrimaryKeyLoadStrategyParam::HASH_TABLE);
    return false;
}

template <typename Key, typename DerivedType>
void PrimaryKeyReader<Key, DerivedType>::CreateLoadPlans(
    std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
    std::vector<std::unique_ptr<PrimaryKeyLoadPlan>>& plans,
    const std::shared_ptr<config::InvertedIndexConfig>& indexConfig)
{
    auto pkIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
    auto param = pkIndexConfig->GetPKLoadStrategyParam();
    docid64_t baseDocid = 0;
    if (param.NeedCombineSegments()) {
        std::unique_ptr<PrimaryKeyLoadPlan> plan = std::make_unique<PrimaryKeyLoadPlan>();
        plan->Init(baseDocid);
        auto PushBackPlan = [&](std::unique_ptr<PrimaryKeyLoadPlan>& plan) -> void {
            baseDocid += plan->GetDocCount();
            plans.push_back(std::move(plan));
            plan = std::make_unique<PrimaryKeyLoadPlan>();
            plan->Init(baseDocid);
        };
        auto DoCreateLoadPlans = [&](bool createRTLoadPlans) -> void {
            for (const auto& segmentData : segmentDatas) {
                if (segmentData._isRTSegment != createRTLoadPlans) {
                    continue;
                }
                plan->AddSegmentData(segmentData);
                if (plan->GetDocCount() >= param.GetMaxDocCount()) {
                    PushBackPlan(plan);
                }
            }
            if (plan->GetDocCount()) {
                PushBackPlan(plan);
            }
        };
        DoCreateLoadPlans(false);
        // create rt segment load plan secondly
        DoCreateLoadPlans(true);
    } else {
        for (auto segmentData : segmentDatas) {
            std::unique_ptr<PrimaryKeyLoadPlan> plan = std::make_unique<PrimaryKeyLoadPlan>();
            plan->Init(segmentData._baseDocId);
            plan->AddSegmentData(segmentData);
            plans.push_back(std::move(plan));
        }
    }
}

template <typename Key, typename DerivedType>
std::shared_ptr<indexlib::file_system::FileWriter>
PrimaryKeyReader<Key, DerivedType>::CreateTargetSliceFile(PrimaryKeyLoadPlan* plan, const std::string& fileName)
{
    // TODO(xiaohao.yxh) combined not work wi
    AUTIL_LOG(INFO, "Begin to create pk slice file: [%s]", fileName.c_str());
    auto [status, directory] = plan->GetPrimaryKeyDirectory(_primaryKeyIndexConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get primary key directory failed");
        return nullptr;
    }
    auto [st, pkDataDir] = directory->GetDirectory(_primaryKeyIndexConfig->GetIndexName()).StatusWith();
    if (!st.IsOK() || nullptr == pkDataDir) {
        AUTIL_LOG(ERROR, "fail to get primary key data directory");
        return nullptr;
    }
    std::unique_ptr<PrimaryKeyIterator<Key>> pkIterator(new PrimaryKeyIterator<Key>());
    if (!pkIterator->Init(_primaryKeyIndexConfig, plan->GetLoadSegmentDatas())) {
        AUTIL_LOG(ERROR, "create PrimaryKeyIterator for directory [%s] failed", directory->DebugString().c_str());
        return nullptr;
    }
    uint64_t pkCount = pkIterator->GetPkCount();
    uint64_t docCount = pkIterator->GetDocCount();
    size_t sliceFileLen = 0;
    try {
        sliceFileLen =
            index::PrimaryKeyHashTable<Key>::CalculateMemorySize(pkIterator->GetPkCount(), plan->GetDocCount());
    } catch (...) {
        AUTIL_LOG(ERROR, "Failed to calculate target slice file load size [%s]", fileName.c_str());
        return nullptr;
    }
    auto [writerStatus, sliceFileWriter] =
        pkDataDir->CreateFileWriter(fileName, indexlib::file_system::WriterOption::Slice(sliceFileLen, 1)).StatusWith();
    if (!writerStatus.IsOK() || !sliceFileWriter) {
        AUTIL_LOG(ERROR, "create slice file for file [%s] failed", fileName.c_str());
        return nullptr;
    }
    sliceFileWriter->Truncate(sliceFileLen).GetOrThrow();
    char* baseAddress = (char*)sliceFileWriter->GetBaseAddress();
    index::PrimaryKeyHashTable<Key> pkHashTable;
    pkHashTable.Init(baseAddress, pkCount, docCount);
    PKPairTyped pkPair;
    while (pkIterator->HasNext()) {
        if (!pkIterator->Next(pkPair)) {
            sliceFileWriter->Close().GetOrThrow();
            return nullptr;
        }
        SegmentPKPairTyped pkPair32;
        pkPair32.key = pkPair.key;
        pkPair32.docid = pkPair.docid;
        pkHashTable.Insert(pkPair32);
    }
    AUTIL_LOG(INFO, "Finish to create pk slice file: [%s]", fileName.c_str());
    return sliceFileWriter;
}

template <typename Key, typename DerivedType>
size_t PrimaryKeyReader<Key, DerivedType>::EstimateLoadSize(const Segments& allSegments,
                                                            const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    std::vector<SegmentDataAdapter::SegmentDataType> segmentDatas;
    auto pkConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
    assert(pkConfig);
    SegmentDataAdapter::Transform(allSegments, segmentDatas);
    return DoEstimateLoadSize(segmentDatas, pkConfig, /*isValidVersion*/ false);
}

template <typename Key, typename DerivedType>
size_t
PrimaryKeyReader<Key, DerivedType>::DoEstimateLoadSize(std::vector<SegmentDataAdapter::SegmentDataType>& segmentDatas,
                                                       const std::shared_ptr<config::InvertedIndexConfig>& indexConfig,
                                                       bool isValidVersion)
{
    std::vector<std::unique_ptr<PrimaryKeyLoadPlan>> plans;
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> pkIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);

    CreateLoadPlans(segmentDatas, plans, indexConfig);
    size_t totalLoadSize = 0;
    for (auto& plan : plans) {
        std::string fileName(PRIMARY_KEY_DATA_FILE_NAME);

        auto [status, dir] = plan->GetPrimaryKeyDirectory(indexConfig);
        if (!status.IsOK() || !dir) {
            AUTIL_LOG(ERROR, "get primary key directory failed");
            AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "get primary key directory failed");
        }
        auto [st, pkDataDir] = dir->GetDirectory(indexConfig->GetIndexName()).StatusWith();
        if (!st.IsOK() || !pkDataDir) {
            AUTIL_LOG(ERROR, "get primary key data directory failed");
            AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "get primary key directory failed");
        }
        if (!CanDirectLoad(indexConfig) || plan->GetSegmentNum() > 1) {
            fileName = plan->GetTargetSliceFileName();
            PrimaryKeyIterator<Key> iter;
            if (!iter.Init(pkIndexConfig, plan->GetLoadSegmentDatas())) {
                AUTIL_LOG(ERROR, "create PrimaryKeyIterator for directory [%s] failed", dir->DebugString().c_str());
                AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "create PrimaryKeyIterator failed");
            }
            if (isValidVersion && pkDataDir->IsExistInCache(fileName)) {
                continue;
            }
            totalLoadSize +=
                index::PrimaryKeyHashTable<Key>::CalculateMemorySize(iter.GetPkCount(), iter.GetDocCount());
        } else {
            if (isValidVersion && pkDataDir->IsExistInCache(fileName)) {
                continue;
            }
            totalLoadSize += index::PrimaryKeyDiskIndexer<Key>::CalculateLoadSize(pkIndexConfig, pkDataDir, fileName);
        }
    }
    return totalLoadSize;
}

template <typename Key, typename DerivedType>
indexlib::index::Result<indexlib::index::PostingIterator*>
PrimaryKeyReader<Key, DerivedType>::Lookup(const indexlib::index::Term& term, uint32_t statePoolSize, PostingType type,
                                           autil::mem_pool::Pool* sessionPool)
{
    Key hashKey;
    dictkey_t termHashKey;
    if (term.GetTermHashKey(termHashKey)) {
        hashKey = (Key)termHashKey;

    } else if (!Hash(term.GetWord(), hashKey)) {
        return NULL;
    }
    // TODO(xiaohao.yxh) remove try-catch
    try {
        docid64_t docId = Lookup(hashKey);
        if (docId != INVALID_DOCID) {
            return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, indexlib::index::PrimaryKeyPostingIterator, docId,
                                                sessionPool);
        }
    } catch (...) {
        AUTIL_LOG(ERROR, "lookup pk failed");
        return indexlib::index::ErrorCode::FileIO;
    }
    return NULL;
}

template <typename Key, typename DerivedType>
docid64_t PrimaryKeyReader<Key, DerivedType>::Lookup(const autil::StringView& pkStr) const
{
    Key key;
    if (!indexlib::index::KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, pkStr.data(), pkStr.size(),
                                                       key)) {
        return INVALID_DOCID;
    }
    return Lookup(key);
}

template <typename Key, typename DerivedType>
docid64_t PrimaryKeyReader<Key, DerivedType>::Lookup(const std::string& strKey, future_lite::Executor* executor) const
{
    Key hashKey;
    if (!Hash(strKey, hashKey)) {
        return INVALID_DOCID;
    }
    return Lookup(hashKey, executor);
}

template <typename Key, typename DerivedType>
docid64_t PrimaryKeyReader<Key, DerivedType>::Lookup(const Key& hashKey, docid64_t& lastDocId) const
{
    // look up in memory segment reader first
    docid64_t docId = LookupInMemorySegment(hashKey);
    lastDocId = docId; // INVALID or building docid
    if (IsDocIdValid(docId)) {
        return docId;
    }

    return LookupOnDiskSegments(hashKey, lastDocId);
}

template <typename Key, typename DerivedType>
docid64_t PrimaryKeyReader<Key, DerivedType>::Lookup(const std::string& pkStr, docid64_t& lastDocId) const
{
    Key hashKey;
    if (!Hash(pkStr, hashKey)) {
        return INVALID_DOCID;
    }
    return Lookup(hashKey, lastDocId);
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::LookupWithHintValues(const autil::uint128_t& pkHash,
                                                                          int32_t hintValues) const
{
    if constexpr (std::is_same_v<Key, autil::uint128_t>) {
        return InnerLookupWithHintValues(pkHash, hintValues);
    } else {
        assert((std::is_same_v<Key, uint64_t>));
        uint64_t pkValue = pkHash.value[1];
        return InnerLookupWithHintValues(pkValue, hintValues);
    }
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::LookupWithPKHash(const autil::uint128_t& pkHash,
                                                                      future_lite::Executor* executor) const
{
    if constexpr (std::is_same_v<Key, autil::uint128_t>) {
        return Lookup(pkHash, executor);
    } else {
        assert((std::is_same_v<Key, uint64_t>));
        uint64_t pkValue = pkHash.value[1];
        return Lookup(pkValue, executor);
    }
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::LookupWithPKHash(const autil::uint128_t& pkHash,
                                                                 segmentid_t specifySegment, docid64_t* docid) const
{
    if constexpr (std::is_same_v<Key, autil::uint128_t>) {
        return InnerLookupWithPKHash(pkHash, specifySegment, docid);
    } else {
        assert((std::is_same_v<Key, uint64_t>));
        uint64_t pkValue = pkHash.value[1];
        return InnerLookupWithPKHash(pkValue, specifySegment, docid);
    }
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::LookupWithDocRange(const autil::uint128_t& pkHash,
                                                                        std::pair<docid_t, docid_t> docRange,
                                                                        future_lite::Executor* executor) const
{
    if constexpr (std::is_same_v<Key, autil::uint128_t>) {
        return InnerLookupWithDocRange(pkHash, docRange, executor);
    } else {
        assert((std::is_same_v<Key, uint64_t>));
        uint64_t pkValue = pkHash.value[1];
        return InnerLookupWithDocRange(pkValue, docRange, executor);
    }
}

template <typename Key, typename DerivedType>
bool PrimaryKeyReader<Key, DerivedType>::LookupAll(const std::string& pkStr,
                                                   std::vector<std::pair<docid64_t, bool>>& docidPairVec) const
{
    Key hashKey;
    if (!Hash(pkStr, hashKey)) {
        return false;
    }

    for (const auto& readerInfo : _segmentReaderList) {
        auto retWithEc = future_lite::coro::syncAwait(
            LookupOneSegmentAsync(hashKey, readerInfo._segmentPair.first, readerInfo._segmentPair.second, nullptr));
        docid64_t docId = retWithEc.ValueOrThrow();
        if (docId != INVALID_DOCID) {
            bool isDeleted = DocDeleted(docId);
            docidPairVec.push_back(std::make_pair(docId, isDeleted));
        }
    }
    docid64_t docId = LookupInMemorySegment(hashKey);
    if (docId != INVALID_DOCID) {
        bool isDeleted = DocDeleted(docId);
        docidPairVec.push_back(std::make_pair(docId, isDeleted));
    }
    return true;
}

template <typename Key, typename DerivedType>
docid64_t PrimaryKeyReader<Key, DerivedType>::LookupInMemorySegment(const Key& hashKey) const
{
    if (!_buildingIndexReader) {
        return INVALID_DOCID;
    }
    return _buildingIndexReader->Lookup(hashKey);
}

template <typename Key, typename DerivedType>
inline future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>
PrimaryKeyReader<Key, DerivedType>::LookupOnDiskSegmentsAsync(const Key& hashKey,
                                                              future_lite::Executor* executor) const noexcept
{
    std::vector<future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>> tasks;
    tasks.reserve(_segmentReaderList.size());
    for (const auto& readerInfo : _segmentReaderList) {
        tasks.push_back(
            LookupOneSegmentAsync(hashKey, readerInfo._segmentPair.first, readerInfo._segmentPair.second, executor));
    }
    auto results = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < results.size(); ++i) {
        assert(!results[i].hasError());
        auto ret = results[i].value();
        if (!ret.Ok()) {
            co_return ret.GetErrorCode();
        }
        docid64_t docid = ret.Value();
        if (!IsDocIdValid(docid)) {
            continue;
        }
        co_return docid;
    }
    co_return INVALID_DOCID;
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::LookupOnDiskSegments(const Key& hashKey,
                                                                          future_lite::Executor* executor) const
{
    if (executor) {
        auto retWithEc = future_lite::coro::syncAwait(LookupOnDiskSegmentsAsync(hashKey, executor));
        return retWithEc.ValueOrThrow();
    } else {
        for (const auto& readerInfo : _segmentReaderList) {
            auto retWithEc = LookupOneSegment(hashKey, readerInfo._segmentPair.first, readerInfo._segmentPair.second);
            auto gDocId = retWithEc.ValueOrThrow();
            if (!IsDocIdValid(gDocId)) {
                // for inc cover rt, rt doc deleted use inc doc
                continue;
            }
            return gDocId;
        }
        return INVALID_DOCID;
    }
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::IsDocIdValid(docid64_t docid) const
{
    if (docid == INVALID_DOCID) {
        return false;
    }
    if (DocDeleted(docid)) {
        return false;
    }
    return true;
}

template <typename Key, typename DerivedType>
inline future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>
PrimaryKeyReader<Key, DerivedType>::LookupOneSegmentAsync(const Key& hashKey, docid64_t baseDocid,
                                                          const std::shared_ptr<PrimaryKeyDiskIndexer<Key>>& segReader,
                                                          future_lite::Executor* executor) const noexcept
{
    auto retWithEc = co_await segReader->LookupAsync(hashKey, executor);
    if (!retWithEc.Ok()) {
        co_return retWithEc.GetErrorCode();
    }
    auto localDocId = retWithEc.Value();
    if (localDocId == INVALID_DOCID) {
        co_return INVALID_DOCID;
    }
    co_return baseDocid + localDocId;
}

template <typename Key, typename DerivedType>
inline indexlib::index::Result<docid64_t> PrimaryKeyReader<Key, DerivedType>::LookupOneSegment(
    const Key& hashKey, docid64_t baseDocid,
    const std::shared_ptr<PrimaryKeyDiskIndexer<Key>>& segReader) const noexcept
{
    auto retWithEc = segReader->Lookup(hashKey);
    if (!retWithEc.Ok()) {
        return retWithEc.GetErrorCode();
    }
    auto localDocId = retWithEc.Value();
    if (localDocId == INVALID_DOCID) {
        return INVALID_DOCID;
    }
    return baseDocid + localDocId;
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::LookupOnDiskSegments(const Key& hashKey,
                                                                          docid64_t& lastDocId) const
{
    for (const auto& readerInfo : _segmentReaderList) {
        auto retWithEc = future_lite::coro::syncAwait(
            LookupOneSegmentAsync(hashKey, readerInfo._segmentPair.first, readerInfo._segmentPair.second, nullptr));
        auto gDocId = retWithEc.ValueOrThrow();
        if (lastDocId < gDocId) {
            lastDocId = gDocId;
        }
        if (!IsDocIdValid(gDocId)) {
            // for inc cover rt, rt doc deleted use inc doc
            continue;
        }
        lastDocId = gDocId;
        return gDocId;
    }
    return INVALID_DOCID;
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::Lookup(const Key& hashKey, future_lite::Executor* executor) const
{
    docid64_t docId = INVALID_DOCID;
    if (_needLookupReverse) {
        docId = LookupInMemorySegment(hashKey);
        if (IsDocIdValid(docId)) {
            return docId;
        }
    }

    docId = LookupOnDiskSegments(hashKey, executor);
    if (docId != INVALID_DOCID) {
        return docId;
    }

    if (!_needLookupReverse) {
        docId = LookupInMemorySegment(hashKey);
        if (IsDocIdValid(docId)) {
            return docId;
        }
    }
    return INVALID_DOCID;
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::InnerLookupWithPKHash(const Key& hashKey, segmentid_t specifySegment,
                                                                      docid64_t* docid) const
{
    for (auto& segmentReaderInfo : _segmentReaderList) {
        for (segmentid_t segId : segmentReaderInfo._segmentIds) {
            if (segId == specifySegment) {
                if (segmentReaderInfo._segmentIds.size() > 1) {
                    assert(false);
                    AUTIL_LOG(ERROR, "not support find specify segment doc with combine pk load strategy");
                    return false;
                }
                auto retWithEc = future_lite::coro::syncAwait(LookupOneSegmentAsync(
                    hashKey, segmentReaderInfo._segmentPair.first, segmentReaderInfo._segmentPair.second, nullptr));
                *docid = retWithEc.ValueOrThrow();
                if (*docid == INVALID_DOCID) {
                    AUTIL_LOG(DEBUG, "not find specify segment [%d] doc with pk load strategy", specifySegment);
                    return false;
                }
                if (!IsDocIdValid(*docid)) {
                    *docid = INVALID_DOCID;
                    return true;
                }
                return true;
            }
        }
    }
    if (!_buildingIndexReader) {
        *docid = INVALID_DOCID;
        return false;
    }
    if (!_buildingIndexReader->Lookup(hashKey, specifySegment, docid)) {
        return false;
    }
    if (!IsDocIdValid(*docid)) {
        *docid = INVALID_DOCID;
        return true;
    }
    return true;
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::InnerLookupWithDocRange(
    const Key& pkHash, const std::pair<docid32_t, docid32_t> docRange, future_lite::Executor* executor) const
{
    docid64_t id = INVALID_DOCID;

    if (executor) {
        auto getDocidFunc = [&]() -> future_lite::coro::Lazy<docid64_t> {
            std::vector<future_lite::coro::Lazy<indexlib::index::Result<docid64_t>>> tasks;
            for (auto& segmentReaderInfo : _segmentReaderList) {
                if (segmentReaderInfo._segmentPair.first < docRange.second) {
                    tasks.push_back(LookupOneSegmentAsync(pkHash, segmentReaderInfo._segmentPair.first,
                                                          segmentReaderInfo._segmentPair.second, executor));
                }
            }
            auto results = co_await future_lite::coro::collectAll(std::move(tasks));
            for (size_t i = 0; i < results.size(); ++i) {
                assert(!results[i].hasError());
                docid64_t docId = results[i].value().ValueOrThrow();
                if (docId != INVALID_DOCID && docId >= docRange.first && docId < docRange.second) {
                    if (IsDocIdValid(docId)) {
                        co_return docId;
                    }
                }
            }
            co_return INVALID_DOCID;
        };
        id = future_lite::coro::syncAwait(getDocidFunc());
        if (id != INVALID_DOCID) {
            return id;
        }
    } else {
        for (auto& segmentReaderInfo : _segmentReaderList) {
            if (segmentReaderInfo._segmentPair.first < docRange.second) {
                auto retWithEc = future_lite::coro::syncAwait(LookupOneSegmentAsync(
                    pkHash, segmentReaderInfo._segmentPair.first, segmentReaderInfo._segmentPair.second, executor));
                docid64_t docId = retWithEc.ValueOrThrow();
                if (docId != INVALID_DOCID && docId >= docRange.first && docId < docRange.second) {
                    if (IsDocIdValid(docId)) {
                        return docId;
                    }
                }
            }
        }
    }

    if (!_buildingIndexReader) {
        return INVALID_DOCID;
    }
    id = _buildingIndexReader->Lookup(pkHash);
    if (id != INVALID_DOCID && id >= docRange.first && id < docRange.second) {
        if (IsDocIdValid(id)) {
            return id;
        }
    }
    return INVALID_DOCID;
}

template <typename Key, typename DerivedType>
inline docid64_t PrimaryKeyReader<Key, DerivedType>::InnerLookupWithHintValues(const Key& pkHash,
                                                                               int32_t hintValues) const
{
    DocIdRangeVector docIdRanges;
    bool ret = false;
    if constexpr (std::is_same_v<DerivedType, void>) {
        ret = GetDocIdRanges(hintValues, docIdRanges);
    } else {
        ret = static_cast<const DerivedType*>(this)->GetDocIdRanges(hintValues, docIdRanges);
    }
    if (ret) {
        for (auto& docIdRange : docIdRanges) {
            docid64_t docId = InnerLookupWithDocRange(pkHash, docIdRange, nullptr);
            if (docId != INVALID_DOCID) {
                return docId;
            }
        }
        return INVALID_DOCID;
    } else {
        return LookupWithPKHash(pkHash, nullptr);
    }
    return INVALID_DOCID;
}

template <typename Key, typename DerivedType>
bool PrimaryKeyReader<Key, DerivedType>::GetDocIdRanges(int32_t hintValues, DocIdRangeVector& docIdRanges) const
{
    // TODO: add this logic when indexlibv2 support temperature
    // if (_temperatureDocInfo && _temperatureDocInfo->GetTemperatureDocIdRanges(hintValues, docIdRanges)) {
    //     return true;
    // }
    return false;
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::CheckDuplication() const
{
    AUTIL_LOG(INFO, "primaryKeyReader begin Check Duplication.");

    indexlib::index::PrimaryKeyDuplicationChecker checker(this);
    if (!checker.Start()) {
        AUTIL_LOG(ERROR, "start checker failed");
        return false;
    }

    auto PushKey = [&checker, this](Key key, docid64_t docId) -> bool {
        if (this->DocDeleted(docId)) {
            return true;
        }
        if (!checker.PushKey(key, docId)) {
            AUTIL_LOG(ERROR, "push pk failed");
            return false;
        }
        return true;
    };
    int64_t totalCheckDoc = 0;

    for (const auto& readerInfo : _segmentReaderList) {
        auto baseDocid = readerInfo._segmentPair.first;
        auto leafIter = readerInfo._segmentPair.second->CreatePrimaryKeyLeafIterator();
        if (!leafIter) {
            AUTIL_LOG(ERROR, "create leaf iterator fail for segmentReader with base docid [%ld]", baseDocid);
            return false;
        }

        SegmentPKPairTyped pkPair;
        while (leafIter->HasNext()) {
            if (!leafIter->Next(pkPair).IsOK()) {
                AUTIL_LOG(ERROR, "primaryKeyIter do next failed!");
                return false;
            }
            docid64_t docId = baseDocid + pkPair.docid;
            if (!PushKey(pkPair.key, docId)) {
                return false;
            }
            totalCheckDoc++;
            if (totalCheckDoc % 100000000 == 0) {
                AUTIL_LOG(INFO, "check doc [%ld]", totalCheckDoc);
            }
        }
    }

    if (_buildingIndexReader) {
        for (const auto& [baseDocId, _, segmentReader] : _buildingIndexReader->GetSegReaderItems()) {
            auto iter = segmentReader->CreateIterator();
            while (iter.HasNext()) {
                typename HashMapTyped::KeyValuePair& kvPair = iter.Next();
                docid64_t docId = baseDocId + kvPair.second;
                if (!PushKey(kvPair.first, docId)) {
                    return false;
                }
                totalCheckDoc++;
                if (totalCheckDoc % 100000000 == 0) {
                    AUTIL_LOG(INFO, "check doc [%ld]", totalCheckDoc);
                }
            }
        }
    }

    if (!checker.WaitFinish()) {
        AUTIL_LOG(INFO, "primaryKeyReader Check Duplication failed");
        return false;
    }

    AUTIL_LOG(INFO, "primaryKeyReader Check Duplication pass, totalCheck doc [%ld]", totalCheckDoc);
    return true;
}

template <typename Key, typename DerivedType>
std::pair<Status, std::vector<bool>>
PrimaryKeyReader<Key, DerivedType>::Contains(const std::vector<const document::IIndexFields*>& indexFields) const
{
    std::unordered_set<std::string> pksInBatch;

    auto isDuplicated = [&pksInBatch, this](const auto* indexField) -> bool {
        if (auto* typedFields = dynamic_cast<const PrimaryKeyIndexFields*>(indexField)) {
            const std::string& pkStr = typedFields->GetPrimaryKey();
            if (!pkStr.empty()) {
                if (pksInBatch.count(pkStr)) {
                    AUTIL_INTERVAL_LOG2(60, INFO, "find duplicated pk [%s]", pkStr.c_str());
                    return true;
                }
                pksInBatch.insert(pkStr);

                if (Lookup(pkStr) != INVALID_DOCID) {
                    AUTIL_INTERVAL_LOG2(60, INFO, "find duplicated pk [%s]", pkStr.c_str());
                    return true;
                }
            }
        }
        return false;
    };

    std::vector<bool> ret;
    for (auto* indexField : indexFields) {
        try {
            ret.push_back(isDuplicated(indexField));
        } catch (const autil::legacy::ExceptionBase& e) {
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), std::vector<bool> {}, "catch exception [%s]", e.what());
        } catch (...) {
            RETURN2_IF_STATUS_ERROR(Status::Corruption(), std::vector<bool> {}, "catch unknow exception ");
        }
    }
    return {Status::OK(), ret};
}

// template <typename Key, typename DerivedType>
// bool PrimaryKeyReader<Key, DerivedType>::GetLocalDocidInfo(docid64_t gDocId, std::string& segIds,
//                                                            docid64_t& localDocid) const
// {
//     docid64_t baseDocid = INVALID_DOCID;
//     if (gDocId >= _baseDocid && _buildingIndexReader) {
//         // building segments
//         baseDocid = _baseDocid;
//         segmentid_t targetSegId = INVALID_SEGMENTID;
//         for (const auto& [currentBaseDocId, segmentId, segmentReader] : _buildingIndexReader->GetSegReaderItems()) {
//             if (currentBaseDocId > gDocId) {
//                 break;
//             }
//             baseDocid = currentBaseDocId;
//             targetSegId = segmentId;
//         }

//         if (targetSegId == INVALID_SEGMENTID) {
//             return false;
//         }
//         segIds = autil::StringUtil::toString(targetSegId);
//         localDocid = gDocId - baseDocid;
//         return true;
//     }

//     // built segments
//     size_t idx = 0;
//     for (size_t i = 0; i < _segmentReaderList.size(); i++) {
//         docid64_t tmpBaseDocid = _segmentReaderList[i]._segmentPair.first;
//         if (gDocId < tmpBaseDocid) {
//             continue;
//         }
//         if (tmpBaseDocid >= baseDocid) {
//             baseDocid = tmpBaseDocid;
//             idx = i;
//         }
//     }

//     if (baseDocid == INVALID_DOCID) {
//         return false;
//     }
//     segIds = autil::StringUtil::toString(_segmentReaderList[idx]._segmentIds, ",");
//     localDocid = gDocId - baseDocid;
//     return true;
// }

template <typename Key, typename DerivedType>
size_t PrimaryKeyReader<Key, DerivedType>::EvaluateCurrentMemUsed()
{
    size_t totalSize = 0;
    for (const auto& readerInfo : _segmentReaderList) {
        totalSize += readerInfo._segmentPair.second->EvaluateCurrentMemUsed();
    }
    return totalSize;
}

template <typename Key, typename DerivedType>
inline bool PrimaryKeyReader<Key, DerivedType>::DocDeleted(docid64_t docid) const
{
    if constexpr (std::is_same_v<DerivedType, void>) {
        return IsDocDeleted(docid);
    } else {
        return static_cast<const DerivedType*>(this)->IsDocDeleted(docid);
    }
}

} // namespace indexlibv2::index
