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
#include "indexlib/index/kkv/building/KKVMemIndexer.h"

#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/kkv/common/KKVSegmentStatistics.h"
#include "indexlib/index/kkv/common/KKVTimestampHelper.h"
#include "indexlib/index/kkv/dump/KKVIndexDumper.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, KKVMemIndexer, SKeyType);

template <typename SKeyType>
KKVMemIndexer<SKeyType>::KKVMemIndexer(const std::string& tabletName, size_t maxPKeyMemUse, size_t maxSKeyMemUse,
                                       size_t maxValueMemUse, double skeyCompressRatio, double valueCompressRatio,
                                       bool isOnline)
    : _tabletName(tabletName)
    , _maxPKeyMemUse(maxPKeyMemUse)
    , _maxSKeyMemUse(maxSKeyMemUse)
    , _maxValueMemUse(maxValueMemUse)
    , _skeyCompressRatio(skeyCompressRatio)
    , _valueCompressRatio(valueCompressRatio)
    , _isOnline(isOnline)
{
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                     document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    if (!kkvIndexConfig) {
        return Status::InvalidArgs("not an kv index config");
    }

    const auto& valueConfig = kkvIndexConfig->GetValueConfig();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    if (!status.IsOK() || !packAttrConfig) {
        return Status::ConfigError("create pack attribute config failed for index: %s",
                                   indexConfig->GetIndexName().c_str());
    }
    // note: parser always pack attribute for historical reasons
    _attrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig));
    _plainFormatEncoder.reset(PackAttributeFormatter::CreatePlainFormatEncoder(packAttrConfig));

    _indexName = kkvIndexConfig->GetIndexName();
    _indexNameHash = config::IndexConfigHash::Hash(kkvIndexConfig);
    _fixValueLen = valueConfig->GetFixedLength();

    auto pkeyCount = EstimatePkeyCount(_maxPKeyMemUse, kkvIndexConfig.get());

    // TODO(xinfei.sxf) check the _maxPKeyMemUse param for PrefixKeyTableCreator Create api
    _pkeyTable.reset(PrefixKeyTableCreator<SKeyListInfo>::Create(
        nullptr, kkvIndexConfig->GetIndexPreference(), index::PKeyTableOpenType::RW, pkeyCount, _maxPKeyMemUse));
    _skeyWriter.reset(new SKeyWriter<SKeyType>);
    _skeyWriter->Init(_maxSKeyMemUse, kkvIndexConfig->GetSuffixKeySkipListThreshold(),
                      kkvIndexConfig->GetSuffixKeyProtectionThreshold());

    status = InitValueWriter(_maxValueMemUse);
    RETURN_IF_STATUS_ERROR(status, "init value writer failed");

    _indexConfig = std::move(kkvIndexConfig);
    AUTIL_LOG(INFO, "table [%s] index [%s] init success", _tabletName.c_str(), _indexName.c_str());
    return Status::OK();
}

template <typename SKeyType>
inline Status KKVMemIndexer<SKeyType>::InitValueWriter(size_t maxValueMemUse)
{
    const uint64_t kMaxSliceLen = 32 * 1024 * 1024; // 32MB;
    uint64_t sliceLen = (maxValueMemUse > kMaxSliceLen) ? kMaxSliceLen : maxValueMemUse;
    size_t sliceNum = maxValueMemUse / sliceLen;
    _valueWriter.reset(new KKVValueWriter(false));
    return _valueWriter->Init(sliceLen, sliceNum);
}

template <typename SKeyType>
inline size_t KKVMemIndexer<SKeyType>::EstimatePkeyCount(size_t pkeyMemUse, config::KKVIndexConfig* indexConfig)
{
    auto& hashDictParam = indexConfig->GetIndexPreference().GetHashDictParam();
    const std::string& hashTypeStr = hashDictParam.GetHashType();
    int32_t occupancyPct = hashDictParam.GetOccupancyPct();
    PKeyTableType type = GetPrefixKeyTableType(hashTypeStr);

    switch (type) {
    case PKeyTableType::SEPARATE_CHAIN:
        return SeparateChainPrefixKeyTable<SKeyListInfo>::EstimateCapacity(PKeyTableOpenType::RW, pkeyMemUse);
    case PKeyTableType::DENSE:
        return ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>::EstimateCapacity(PKeyTableOpenType::RW,
                                                                                              pkeyMemUse, occupancyPct);
    case PKeyTableType::CUCKOO:
        return ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>::EstimateCapacity(
            PKeyTableOpenType::RW, pkeyMemUse, std::min(80, occupancyPct));
    default:
        assert(false);
    }
    return 0;
}

template <typename SKeyType>
void KKVMemIndexer<SKeyType>::UpdateBuildResourceMetrics(size_t& valueLen)
{
    // TODO(xinfei.sxf) add metrics
    if (valueLen > _maxValueLen) {
        _maxValueLen = valueLen;
    }
}
template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::Build(document::IDocumentBatch* docBatch)
{
    document::KKVDocumentBatch* kkvDocBatch = dynamic_cast<document::KKVDocumentBatch*>(docBatch);
    auto indexNameHash = GetIndexNameHash();
    if (kkvDocBatch == nullptr) {
        return Status::InternalError("table [%s] only support KKVDocumentBatch for kkv index", _tabletName.c_str());
    }

    size_t valueSize = 0;
    for (size_t i = 0; i < kkvDocBatch->GetBatchSize(); ++i) {
        if (kkvDocBatch->IsDropped(i)) {
            continue;
        }
        auto kkvDoc = std::dynamic_pointer_cast<document::KKVDocument>((*kkvDocBatch)[i]);
        auto curIndexNameHash = kkvDoc->GetIndexNameHash();
        if (curIndexNameHash != 0 && indexNameHash != curIndexNameHash) {
            continue;
        }
        auto s = BuildSingleDocument(kkvDoc.get(), valueSize);
        if (!s.IsOK()) {
            return s;
        }
    }

    UpdateBuildResourceMetrics(valueSize);

    return Status::OK();
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::BuildSingleDocument(const document::KKVDocument* doc, size_t& valueSize)
{
    if (IsFull()) {
        AUTIL_LOG(WARN, "table [%s] index [%s] MemIndexer is full.", _tabletName.c_str(), _indexName.c_str());
        return Status::NeedDump();
    }

    auto docType = doc->GetDocOperateType();
    Status status;
    switch (docType) {
    case ADD_DOC:
        status = AddDocument(doc);
        break;
    case DELETE_DOC:
        status = DeleteDocument(doc);
        break;
    default:
        return Status::InternalError("unsupported doc type: %d", docType);
    }

    valueSize += doc->GetValue().length();
    return status;
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::AddDocument(const document::KKVDocument* doc)
{
    if (!doc->HasSKey()) {
        ERROR_COLLECTOR_LOG(ERROR, "table [%s] index [%s] add doc with pkey hash [%lu] has no skey",
                            _tabletName.c_str(), _indexName.c_str(), doc->GetPKeyHash());
        if (!_indexConfig->DenyEmptySuffixKey()) {
            return Status::OK();
        }
        return Status::InvalidArgs("add doc has no skey");
    }

    auto pkey = doc->GetPKeyHash();
    auto skey = doc->GetSKeyHash();
    auto timestamp = KKVTimestampHelper::NormalizeTimestamp(doc->GetUserTimestamp());
    auto value = doc->GetValue();
    RETURN_STATUS_DIRECTLY_IF_ERROR(DecodeDocValue(value));
    auto expireTime = GetExpireTime(doc);

    if (unlikely(value.size() >= index::MAX_KKV_VALUE_LEN)) {
        AUTIL_LOG(WARN, "table [%s] index [%s] insert pkey[%lu] skey[%s] failed, value length [%lu] over limit [%lu]!",
                  _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str(),
                  value.size(), index::MAX_KKV_VALUE_LEN);
        ERROR_COLLECTOR_LOG(WARN,
                            "table [%s] index [%s] insert pkey[%lu] skey[%s] failed, "
                            "value length [%lu] over limit [%lu]!",
                            _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str(),
                            value.size(), index::MAX_KKV_VALUE_LEN);
        return Status::Corruption("value length over limit");
    }

    if (unlikely(value.size() + _valueWriter->GetUsedBytes() > _valueWriter->GetReserveBytes())) {
        AUTIL_LOG(WARN,
                  "table [%s] index [%s] insert pkey[%lu] skey[%s] failed, value memory not enough, "
                  "reserve[%lu], used[%lu], need[%lu]",
                  _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str(),
                  _valueWriter->GetReserveBytes(), _valueWriter->GetUsedBytes(), value.size());
        return Status::NeedDump();
    }

    uint64_t valueOffset = 0;
    auto status = _valueWriter->Append(value, valueOffset);
    RETURN_IF_STATUS_ERROR(status, "append value failed");
    uint32_t skeyOffset = _skeyWriter->Append(
        typename SKeyWriter<SKeyType>::SKeyNode(skey, valueOffset, timestamp, expireTime, index::INVALID_SKEY_OFFSET));
    SKeyListInfo* listInfo = PrefixKeyTableSeeker::FindForRW(_pkeyTable.get(), pkey);

    MEMORY_BARRIER();
    if (listInfo) {
        if (!_skeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            AUTIL_LOG(WARN, "table [%s] index [%s] link skeyNode key[%lu] skey[%s] to skeyWriter failed, no space!",
                      _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str());
            return Status::NeedDump();
        }
    } else if (!_pkeyTable->Insert(pkey, SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1))) {
        AUTIL_LOG(WARN, "table [%s] index [%s] insert key[%lu] to pkeyTable failed, no space!", _tabletName.c_str(),
                  _indexName.c_str(), pkey);
        return Status::NeedDump();
    }
    MEMORY_BARRIER();

    return Status::OK();
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::DecodeDocValue(autil::StringView& value)
{
    if (value.empty()) {
        return Status::OK();
    }

    auto meta = _attrConvertor->Decode(value);
    if (_fixValueLen != -1) {
        autil::StringView tempData = meta.data;
        size_t headerSize = MultiValueAttributeFormatter::GetEncodedCountFromFirstByte(*tempData.data());
        value = autil::StringView(tempData.data() + headerSize, tempData.size() - headerSize);
        assert(!_plainFormatEncoder);
    } else {
        value = meta.data;
        if (_plainFormatEncoder) {
            _memBuffer.Reserve(value.size());
            if (!_plainFormatEncoder->Encode(value, _memBuffer.GetBuffer(), _memBuffer.GetBufferSize(), value)) {
                return Status::InternalError("encode plain format error.");
            }
        }
    }
    return Status::OK();
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::DeleteDocument(const document::KKVDocument* doc)
{
    if (doc->HasSKey()) {
        return this->DeleteSKey(doc);
    }
    return this->DeletePKey(doc);
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::DeleteSKey(const document::KKVDocument* doc)
{
    auto pkey = doc->GetPKeyHash();
    auto skey = doc->GetSKeyHash();
    auto ts = KKVTimestampHelper::NormalizeTimestamp(doc->GetUserTimestamp());

    uint32_t skeyOffset = _skeyWriter->Append(typename SKeyWriter<SKeyType>::SKeyNode(
        skey, INVALID_VALUE_OFFSET, ts, indexlib::UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET));

    SKeyListInfo* listInfo = PrefixKeyTableSeeker::FindForRW(_pkeyTable.get(), pkey);

    MEMORY_BARRIER();
    if (listInfo) {
        if (!_skeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            AUTIL_LOG(WARN, "table [%s] index [%s] link skeyNode key[%lu] skey[%s] to skeyWriter failed, no space!",
                      _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str());
            return Status::NeedDump();
        }
    } else if (!_pkeyTable->Insert(pkey, SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1))) {
        AUTIL_LOG(WARN, "table [%s] index [%s] insert key[%lu] to pkeyTable failed, no space!", _tabletName.c_str(),
                  _indexName.c_str(), pkey);
        return Status::NeedDump();
    }
    MEMORY_BARRIER();

    return Status::OK();
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::DeletePKey(const document::KKVDocument* doc)
{
    auto pkey = doc->GetPKeyHash();
    auto skey = doc->GetSKeyHash();
    auto ts = KKVTimestampHelper::NormalizeTimestamp(doc->GetUserTimestamp());
    uint32_t skeyOffset = _skeyWriter->Append(
        typename SKeyWriter<SKeyType>::SKeyNode(std::numeric_limits<SKeyType>::min(), SKEY_ALL_DELETED_OFFSET, ts,
                                                indexlib::UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET));

    SKeyListInfo* listInfo = PrefixKeyTableSeeker::FindForRW(_pkeyTable.get(), pkey);

    MEMORY_BARRIER();
    if (listInfo) {
        if (!_skeyWriter->LinkSkeyNode(*listInfo, skeyOffset)) {
            AUTIL_LOG(WARN, "table [%s] index [%s] link skeyNode key[%lu] skey[%s] to skeyWriter failed, no space!",
                      _tabletName.c_str(), _indexName.c_str(), pkey, autil::StringUtil::toString(skey).c_str());
            return Status::NeedDump();
        }
    } else if (!_pkeyTable->Insert(pkey, SKeyListInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1))) {
        AUTIL_LOG(WARN, "table [%s] index [%s] insert key[%lu] to pkeyTable failed, no space!", _tabletName.c_str(),
                  _indexName.c_str(), pkey);
        return Status::NeedDump();
    }
    MEMORY_BARRIER();

    return Status::OK();
}

template <typename SKeyType>
std::shared_ptr<KKVBuildingSegmentReader<SKeyType>> KKVMemIndexer<SKeyType>::CreateInMemoryReader() const
{
    return std::make_shared<KKVBuildingSegmentReader<SKeyType>>(_indexConfig, _pkeyTable, _skeyWriter, _valueWriter);
}

template <typename SKeyType>
bool KKVMemIndexer<SKeyType>::IsFull()
{
    if (_pkeyTable && _pkeyTable->IsFull()) {
        return true;
    }
    if (_skeyWriter && _skeyWriter->IsFull()) {
        return true;
    }
    return false;
}

template <typename SKeyType>
Status KKVMemIndexer<SKeyType>::Dump(autil::mem_pool::PoolBase* dumpPool,
                                     const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                     const std::shared_ptr<framework::DumpParams>& params)
{
    KKVIndexDumper<SKeyType> dumper(_pkeyTable, _skeyWriter, _valueWriter, _indexConfig, _isOnline);
    indexlib::file_system::DirectoryOption dirOption;
    auto result = indexDirectory->GetIDirectory()->MakeDirectory(_indexName, dirOption);
    if (!result.OK()) {
        AUTIL_LOG(ERROR, "failed to make_dir:[%s] with error:[%s]", _indexName.c_str(),
                  result.Status().ToString().c_str());
        return Status::InternalError();
    }
    return dumper.Dump(result.Value(), dumpPool);
}

template <typename SKeyType>
void KKVMemIndexer<SKeyType>::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    document::KKVDocumentBatch* kkvDocBatch = dynamic_cast<document::KKVDocumentBatch*>(docBatch);
    if (kkvDocBatch == nullptr) {
        AUTIL_LOG(ERROR, "expect a KKVDocumentBatch");
        return;
    }
    for (size_t i = 0; i < kkvDocBatch->GetBatchSize(); ++i) {
        auto doc = (*kkvDocBatch)[i].get();
        auto docType = doc->GetDocOperateType();
        if (docType != ADD_DOC && docType != DELETE_DOC) {
            AUTIL_LOG(WARN, "table [%s] index [%s] only support add & delete doc for kkv index now",
                      _tabletName.c_str(), _indexName.c_str());
            kkvDocBatch->DropDoc(i);
        }
    }
}

template <typename SKeyType>
bool KKVMemIndexer<SKeyType>::IsValidDocument(document::IDocument* doc)
{
    auto docType = doc->GetDocOperateType();
    if (docType != ADD_DOC && docType != DELETE_DOC) {
        AUTIL_LOG(WARN, "table [%s] index [%s] only support add & delete doc for kkv index now", _tabletName.c_str(),
                  _indexName.c_str());
        return false;
    }
    return true;
}

template <typename SKeyType>
bool KKVMemIndexer<SKeyType>::IsValidField(const document::IIndexFields* fields)
{
    assert(0);
    return false;
}

template <typename SKeyType>
size_t KKVMemIndexer<SKeyType>::EstimateDumpFileSize() const
{
    KKVIndexDumper<SKeyType> dumper(_pkeyTable, _skeyWriter, _valueWriter, _indexConfig, _isOnline);
    return dumper.EstimateDumpFileSize(true, _skeyCompressRatio, _valueCompressRatio);
}

template <typename SKeyType>
size_t KKVMemIndexer<SKeyType>::EstimateDumpTmpMemUse() const
{
    KKVIndexDumper<SKeyType> dumper(_pkeyTable, _skeyWriter, _valueWriter, _indexConfig, _isOnline);
    return dumper.EstimateDumpTmpMemUse(_maxValueLen);
}

template <typename SKeyType>
void KKVMemIndexer<SKeyType>::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    KKVSegmentStatistics stat;

    size_t dumpPkeyFileSize = index::PKeyDumper::GetTotalDumpSize(_pkeyTable);
    size_t skeyMemUse = _skeyWriter->GetUsedBytes();
    size_t valueMemUse = _valueWriter->GetUsedBytes();

    stat.totalMemoryUse = EstimateDumpFileSize();
    stat.pkeyMemoryUse = dumpPkeyFileSize;
    stat.pkeyCount = _pkeyTable->Size();
    if (stat.totalMemoryUse == 0) {
        stat.pkeyMemoryRatio = DEFAULT_PKEY_MEMORY_USE_RATIO;
    } else {
        stat.pkeyMemoryRatio = 1.0 * stat.pkeyMemoryUse / stat.totalMemoryUse;
    }

    stat.skeyMemoryUse = skeyMemUse;
    stat.skeyCount = _skeyWriter->GetTotalSkeyCount();
    stat.maxSKeyCount = _skeyWriter->GetMaxSkeyCount();
    if (skeyMemUse + valueMemUse == 0) {
        stat.skeyValueMemoryRatio = DEFAULT_SKEY_VALUE_MEM_RATIO;
    } else {
        stat.skeyValueMemoryRatio = 1.0 * skeyMemUse / (skeyMemUse + valueMemUse);
    }
    stat.skeyCompressRatio = 1.0;

    stat.valueMemoryUse = valueMemUse;
    stat.maxValueLen = _maxValueLen;

    stat.Store(segmentMetrics.get(), GetIndexName());
}

template <typename SKeyType>
void KKVMemIndexer<SKeyType>::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    size_t pkeyMemUse = _pkeyTable->GetTotalMemoryUse();
    size_t skeyMemUse = _skeyWriter->GetUsedBytes();
    size_t valueMemUse = _valueWriter->GetUsedBytes();
    size_t memBufferMemUse = _memBuffer.GetBufferSize();
    size_t totalMemUse = pkeyMemUse + skeyMemUse + valueMemUse + memBufferMemUse;
    memUpdater->UpdateCurrentMemUse(totalMemUse);

    memUpdater->EstimateDumpedFileSize(EstimateDumpFileSize());
    memUpdater->EstimateDumpTmpMemUse(EstimateDumpTmpMemUse());
}

template <typename SKeyType>
void KKVMemIndexer<SKeyType>::Seal()
{
}

template <typename SKeyType>
bool KKVMemIndexer<SKeyType>::IsDirty() const
{
    return true;
}

template <typename SKeyType>
uint32_t KKVMemIndexer<SKeyType>::GetExpireTime(const document::KKVDocument* doc) const
{
    if (!_indexConfig->TTLEnabled() || !_indexConfig->StoreExpireTime()) {
        return indexlib::UNINITIALIZED_EXPIRE_TIME;
    }
    auto docTTL = doc->GetTTL();
    if (docTTL == UNINITIALIZED_TTL) {
        return indexlib::UNINITIALIZED_EXPIRE_TIME;
    }
    uint64_t expireTime = static_cast<uint64_t>(autil::TimeUtility::us2sec(doc->GetUserTimestamp())) + docTTL;
    return expireTime > indexlib::index::MAX_EXPIRE_TIME ? indexlib::index::MAX_EXPIRE_TIME : expireTime;
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVMemIndexer);

} // namespace indexlibv2::index
