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
#include "indexlib/file_system/file/HintCompressDataDumper.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MmapPool.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, HintCompressDataDumper);

HintCompressDataDumper::HintCompressDataDumper(const std::shared_ptr<FileWriter>& fileWriter,
                                               const std::shared_ptr<FileWriter>& infoWriter,
                                               const std::shared_ptr<FileWriter>& metaWriter,
                                               FileSystemMetricsReporter* reporter)
    : CompressDataDumper(fileWriter, infoWriter, metaWriter, reporter)
    , _trainHintBlockCount(0)
    , _useHintSavedSize(0)
    , _hintDataArray(nullptr)
    , _hintDataLen(0)
    , _disableHintCounter(0)
    , _alwaysAdaptiveCompress(false)
    , _alwaysHintCompress(false)
{
}

HintCompressDataDumper::~HintCompressDataDumper() { DELETE_AND_SET_NULL(_hintDataArray); }

FSResult<void> HintCompressDataDumper::Init(const string& compressorName, size_t bufferSize,
                                            const KeyValueMap& param) noexcept
{
    RETURN_IF_FS_ERROR(CompressDataDumper::Init(compressorName, bufferSize, param), "init CompressDataDumper failed");
    _kmonTags.MergeTags(&_normalTags);
    _normalTags.DelTag(COMPRESS_ENABLE_HINT_PARAM_NAME);
    _normalTags.AddTag(COMPRESS_ENABLE_HINT_PARAM_NAME, "false");
    _trainHintBlockCount =
        GetTypeValueFromKeyValueMap(param, COMPRESS_HINT_SAMPLE_BLOCK_COUNT, DEFAULT_SAMPLE_BLOCK_COUNT);
    float sampleRatio = GetTypeValueFromKeyValueMap(param, COMPRESS_HINT_SAMPLE_RATIO, DEFAULT_SAMPLE_RATIO);
    size_t hintDataCapacity = GetTypeValueFromKeyValueMap(param, COMPRESS_HINT_DATA_CAPACITY, (size_t)0);
    _alwaysAdaptiveCompress = GetTypeValueFromKeyValueMap(param, COMPRESS_HINT_ALWAYS_ADAPTIVE, false);
    _alwaysHintCompress = GetTypeValueFromKeyValueMap(param, "hint_always_use_hint", false);
    RETURN_IF_FS_EXCEPTION(_hintCompressor = CreateCompressor(compressorName, bufferSize, param),
                           "CreateCompressor failed");
    assert(_hintCompressor);
    _hintTrainer.reset(
        _hintCompressor->CreateTrainer(_bufferSize, _trainHintBlockCount, hintDataCapacity, sampleRatio));
    if (_hintTrainer) {
        AUTIL_LOG(INFO,
                  "Init hint trainer for compressor [%s] success, with parameter:"
                  " blockSize [%lu], trainBlockCount [%lu], sampleRatio [%f], target file [%s]",
                  compressorName.c_str(), _bufferSize, _trainHintBlockCount, sampleRatio,
                  _dataWriter->GetLogicalPath().c_str());
    } else {
        AUTIL_LOG(WARN, "create null compress hint trainer for compressor [%s]", compressorName.c_str());
    }
    _hintMetaVec.reserve(1024);
    _useHintSavedSize = 0;
    DELETE_AND_SET_NULL(_hintDataArray);
    _hintDataArray = new SliceArray(SLICE_LEN, SLICE_NUM, new MmapPool, true);
    _hintDataLen = 0;
    ResetDisableHintCounter();
    return FSEC_OK;
}

void HintCompressDataDumper::FlushCompressorData() noexcept(false)
{
    if (!_hintTrainer) {
        CompressDataDumper::FlushCompressorData();
        return;
    }

    assert(_compressor);
    if (_compressor->GetBufferInLen() == 0) {
        // empty buffer
        return;
    }

    assert(_hintTrainer->GetCurrentBlockCount() < _hintTrainer->GetMaxTrainBlockCount());
    if (!_hintTrainer->AddOneBlockData(_compressor->GetBufferIn(), _compressor->GetBufferInLen())) {
        INDEXLIB_FATAL_ERROR(Runtime, "Add data to hint trainer fail, data length [%u], current block count [%lu]",
                             _compressor->GetBufferInLen(), _hintTrainer->GetCurrentBlockCount());
    }
    _compressor->Reset();
    if (_hintTrainer->GetCurrentBlockCount() == _hintTrainer->GetMaxTrainBlockCount()) {
        TrainAndCompressData();
    }
}

FSResult<void> HintCompressDataDumper::Close() noexcept
{
    if (!_compFileAddrMapper) {
        return FSEC_OK;
    }

    RETURN_IF_FS_EXCEPTION(FlushCompressorData(), "FlushCompressorData failed");
    RETURN_IF_FS_EXCEPTION(TrainAndCompressData(), "TrainAndCompressData failed");

    std::shared_ptr<FileWriter> writer = (_metaWriter != nullptr) ? _metaWriter : _dataWriter;

    size_t addrMapDataLen = 0;
    // flush data file
    RETURN_IF_FS_EXCEPTION((addrMapDataLen = _compFileAddrMapper->Dump(writer, _encodeCompressAddressMapper)),
                           "Dump failed");
    // flush hint file
    size_t hintDataLen = 0;
    RETURN_IF_FS_EXCEPTION((hintDataLen = FlushHintFile(writer)), "FlushHintFile failed");

    RETURN_IF_FS_ERROR(_dataWriter->Close(), "close data writer failed");

    if (_metaWriter) {
        RETURN_IF_FS_ERROR(_metaWriter->Close(), "close meta writer failed");
    }

    // flush info file
    KeyValueMap addInfo = _compressParam;
    addInfo["use_hint_block_count"] = autil::StringUtil::toString(_compFileAddrMapper->GetUseHintBlockCount());
    addInfo["use_hint_saved_size"] = autil::StringUtil::toString(_useHintSavedSize);
    addInfo["address_mapper_data_size"] = autil::StringUtil::toString(addrMapDataLen);
    addInfo["hint_data_size"] = autil::StringUtil::toString(hintDataLen);
    RETURN_IF_FS_EXCEPTION(FlushInfoFile(addInfo, _encodeCompressAddressMapper), "FlushInfoFile failed");
    _compFileAddrMapper.reset();
    return FSEC_OK;
}

HintCompressDataDumper::SampleResult HintCompressDataDumper::SampleCompress(const autil::StringView& hintData,
                                                                            size_t& sampleBlockNum,
                                                                            int64_t& useHintBlockCount,
                                                                            int64_t& hintSaveSize)
{
    const size_t SAMPLE_MULTIPLE_NUM = 16;
    const float NO_HINT_BLOCK_RATIO_THRESHOLD = 0.2f;
    const float ALWAYS_HINT_BLOCK_RATIO_THRESHOLD = 0.8f;
    const float DISABLE_HINT_SAVE_DATA_RATIO_THRESHOLD = 0.1f;

    useHintBlockCount = 0;
    sampleBlockNum = _hintTrainer->GetCurrentBlockCount() / SAMPLE_MULTIPLE_NUM;
    int64_t lastHintSaveSize = _useHintSavedSize;
    int64_t lastCompressFileLen = _dataWriter->GetLength();

    // 头部block采样，进行自适应压缩（代价稍大）
    for (size_t j = 0; j < sampleBlockNum; j++) {
        autil::StringView data = _hintTrainer->GetBlockData(j);
        bool useHint = false;
        AdaptiveCompressBlock(hintData, data, useHint);
        if (useHint) {
            useHintBlockCount++;
        }
    }

    hintSaveSize = _useHintSavedSize - lastHintSaveSize;
    int64_t compressDataLen = _dataWriter->GetLength() - lastCompressFileLen;
    int64_t sampleHintQuota = hintData.size() / SAMPLE_MULTIPLE_NUM;
    float hintSaveRatio = (float)(hintSaveSize - sampleHintQuota) / compressDataLen;

    size_t noHintThreshold = (size_t)(sampleBlockNum * NO_HINT_BLOCK_RATIO_THRESHOLD);
    size_t alwaysHintThreshold = (size_t)(sampleBlockNum * ALWAYS_HINT_BLOCK_RATIO_THRESHOLD);

    if (_alwaysAdaptiveCompress) {
        return SRT_ADAPITVE;
    }
    if (hintData.empty() || useHintBlockCount < noHintThreshold ||
        hintSaveRatio < DISABLE_HINT_SAVE_DATA_RATIO_THRESHOLD) {
        return SRT_NO_NEED_HINT;
    }
    if (useHintBlockCount >= alwaysHintThreshold) {
        return SRT_ALWAYS_HINT;
    }
    return SRT_ADAPITVE;
}

void HintCompressDataDumper::TrainAndCompressData() noexcept(false)
{
    if (!_hintTrainer || _hintTrainer->GetCurrentBlockCount() == 0) {
        return;
    }

    if (NeedAutoDisableHintCompress()) {
        // 优化逻辑：连续发生判定no hint后，一批block将自动不执行训练逻辑
        for (size_t j = 0; j < _hintTrainer->GetCurrentBlockCount(); j++) {
            NoHintCompressBlock(_hintTrainer->GetBlockData(j));
        }
        EndTrainCompressData(autil::StringView::empty_instance(), false);
        RenewDisableHintCounter();
        return;
    }

    auto hintData = TrainHintData();
    size_t sampleBlockNum = 0;
    int64_t sampleUseHintCount = 0;
    int64_t sampleHintSaveSize = 0;
    auto sampleResult = SampleCompress(hintData, sampleBlockNum, sampleUseHintCount, sampleHintSaveSize);
    bool hintUseful = (sampleUseHintCount > 0);
    switch (sampleResult) {
    case SRT_NO_NEED_HINT: {
        for (size_t j = sampleBlockNum; j < _hintTrainer->GetCurrentBlockCount(); j++) {
            NoHintCompressBlock(_hintTrainer->GetBlockData(j));
        }
        RenewDisableHintCounter();
        break;
    }
    case SRT_ALWAYS_HINT: {
        assert(sampleUseHintCount > 0);
        int64_t avgUseHintSaveSize = sampleHintSaveSize / sampleUseHintCount;
        for (size_t j = sampleBlockNum; j < _hintTrainer->GetCurrentBlockCount(); j++) {
            autil::StringView data = _hintTrainer->GetBlockData(j);
            if (HintCompressBlock(hintData, data)) {
                _useHintSavedSize += avgUseHintSaveSize;
                hintUseful = true;
                continue;
            }
            NoHintCompressBlock(data);
        }
        ResetDisableHintCounter();
        break;
    }
    case SRT_ADAPITVE: {
        for (size_t j = sampleBlockNum; j < _hintTrainer->GetCurrentBlockCount(); j++) {
            bool useHint = false;
            AdaptiveCompressBlock(hintData, _hintTrainer->GetBlockData(j), useHint);
            if (useHint) {
                hintUseful = true;
            }
        }
        ResetDisableHintCounter();
        break;
    }
    default:
        assert(false);
    }
    EndTrainCompressData(hintData, hintUseful);
}

size_t HintCompressDataDumper::FlushHintFile(const std::shared_ptr<FileWriter>& fileWriter) noexcept(false)
{
    assert(_hintDataArray);
    assert((int64_t)_hintDataLen == _hintDataArray->GetDataSize());
    size_t beginLen = fileWriter->GetLength();
    for (size_t i = 0; i < _hintDataArray->GetSliceNum(); ++i) {
        THROW_IF_FS_ERROR(fileWriter->Write(_hintDataArray->GetSlice(i), _hintDataArray->GetSliceDataLen(i)).Code(),
                          "Write failed");
    }
    DELETE_AND_SET_NULL(_hintDataArray);

    size_t hintDataCount = _hintMetaVec.size();
    if (!_hintMetaVec.empty()) {
        fileWriter->Write(_hintMetaVec.data(), hintDataCount * sizeof(size_t)).GetOrThrow();
    }
    THROW_IF_FS_ERROR(fileWriter->Write(&_hintDataLen, sizeof(size_t)).Code(), "Write failed");
    THROW_IF_FS_ERROR(fileWriter->Write(&hintDataCount, sizeof(size_t)).Code(), "Write failed");
    THROW_IF_FS_ERROR(fileWriter->Write(&_trainHintBlockCount, sizeof(size_t)).Code(), "Write failed");
    return fileWriter->GetLength() - beginLen;
}

void HintCompressDataDumper::AdaptiveCompressBlock(const autil::StringView& hintData, const autil::StringView& data,
                                                   bool& useHint)
{
    _compressor->Reset();
    _compressor->AddDataToBufferIn(data.data(), data.size());
    {
        ScopedCompressLatencyReporter reporter(_reporter, &_normalTags);
        if (!_compressor->Compress()) { // no hint compress
            INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
        }
    }

    if (hintData.empty()) {
        WriteCompressorData(_compressor, false);
        useHint = false;
        return;
    }

    // hint compress
    _hintCompressor->Reset();
    _hintCompressor->AddDataToBufferIn(data.data(), data.size());
    bool compressHint = false;
    {
        ScopedCompressLatencyReporter reporter(_reporter, &_kmonTags);
        compressHint = _hintCompressor->Compress(hintData);
    }

    if (!compressHint) {
        AUTIL_LOG(ERROR, "compress with hint fail, hint data len [%lu]", hintData.size());
        WriteCompressorData(_compressor, false);
        useHint = false;
        return;
    }

    // choose compressor by compress block size
    size_t avgSizeForEachBlock = hintData.size() / _hintTrainer->GetCurrentBlockCount();
    if (!_alwaysHintCompress &&
        _compressor->GetBufferOutLen() <= (_hintCompressor->GetBufferOutLen() + avgSizeForEachBlock)) {
        WriteCompressorData(_compressor, false);
        useHint = false;
        return;
    }
    _useHintSavedSize += (_compressor->GetBufferOutLen() - _hintCompressor->GetBufferOutLen());
    _compressor->Reset();
    WriteCompressorData(_hintCompressor, true);
    useHint = true;
}

void HintCompressDataDumper::NoHintCompressBlock(const autil::StringView& data)
{
    _compressor->Reset();
    _compressor->AddDataToBufferIn(data.data(), data.size());

    {
        ScopedCompressLatencyReporter reporter(_reporter, &_normalTags);
        if (!_compressor->Compress()) { // no hint compress
            INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
        }
    }
    WriteCompressorData(_compressor, false);
}

bool HintCompressDataDumper::HintCompressBlock(const autil::StringView& hintData, const autil::StringView& data)
{
    if (hintData.empty()) {
        return false;
    }

    _hintCompressor->Reset();
    _hintCompressor->AddDataToBufferIn(data.data(), data.size());
    bool ret = false;
    {
        ScopedCompressLatencyReporter reporter(_reporter, &_kmonTags);
        ret = _hintCompressor->Compress(hintData);
    }

    if (!ret) {
        AUTIL_LOG(ERROR, "compress with hint fail, hint data len [%lu]", hintData.size());
        return false;
    }
    WriteCompressorData(_hintCompressor, true);
    return true;
}

// 连续4次采样判定no hint后，4批后持续到第16批的压缩将自动关闭hint压缩
bool HintCompressDataDumper::NeedAutoDisableHintCompress() const { return _disableHintCounter >= 4; }

void HintCompressDataDumper::RenewDisableHintCounter()
{
    ++_disableHintCounter;
    if (_disableHintCounter >= 16) {
        _disableHintCounter = 0;
    }
}

void HintCompressDataDumper::ResetDisableHintCounter() noexcept { _disableHintCounter = 0; }

autil::StringView HintCompressDataDumper::TrainHintData()
{
    ScopedCompressLatencyReporter reporter(_reporter, &_kmonTags, true);
    _hintTrainer->TrainHintData();
    return _hintTrainer->GetHintData();
}

void HintCompressDataDumper::EndTrainCompressData(const autil::StringView& hintData, bool hintUseful)
{
    _hintMetaVec.push_back(_hintDataLen);
    if (hintUseful) {
        assert(_hintDataArray);
        _hintDataArray->SetList(_hintDataLen, hintData.data(), hintData.size());
        _hintDataLen += hintData.size();
        _useHintSavedSize -= (int64_t)hintData.size();
    }
    _hintTrainer->Reset();
    _compressor->Reset();
}

}} // namespace indexlib::file_system
