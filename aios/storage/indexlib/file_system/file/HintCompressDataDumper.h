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

#include "indexlib/file_system/file/CompressDataDumper.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlib::util {
class BlockCache;
class CompressHintDataTrainer;
} // namespace indexlib::util

namespace indexlib { namespace file_system {

class HintCompressDataDumper : public CompressDataDumper
{
public:
    HintCompressDataDumper(const std::shared_ptr<FileWriter>& fileWriter, const std::shared_ptr<FileWriter>& infoWriter,
                           const std::shared_ptr<FileWriter>& metaWriter, FileSystemMetricsReporter* reporter);

    ~HintCompressDataDumper();

    HintCompressDataDumper(const HintCompressDataDumper&) = delete;
    HintCompressDataDumper& operator=(const HintCompressDataDumper&) = delete;
    HintCompressDataDumper(HintCompressDataDumper&&) = delete;
    HintCompressDataDumper& operator=(HintCompressDataDumper&&) = delete;

public:
    FSResult<void> Init(const std::string& compressorName, size_t bufferSize,
                        const util::KeyValueMap& compressorParam) noexcept override;

    FSResult<void> Close() noexcept override;

public:
    static size_t EstimateCompressBufferSize(const std::string& compressorName, size_t bufferSize,
                                             const util::KeyValueMap& compressorParam) noexcept;

private:
    void FlushCompressorData() noexcept(false) override;
    void TrainAndCompressData() noexcept(false);
    size_t FlushHintFile(const std::shared_ptr<FileWriter>& fileWriter) noexcept(false);

    void AdaptiveCompressBlock(const autil::StringView& hintData, const autil::StringView& data, bool& useHint);
    void NoHintCompressBlock(const autil::StringView& data);
    bool HintCompressBlock(const autil::StringView& hintData, const autil::StringView& data);

    void ResetDisableHintCounter() noexcept;
    bool NeedAutoDisableHintCompress() const;
    void RenewDisableHintCounter();

    autil::StringView TrainHintData();
    void EndTrainCompressData(const autil::StringView& hintData, bool hintUseful);

private:
    enum SampleResult { SRT_NO_NEED_HINT, SRT_ALWAYS_HINT, SRT_ADAPITVE };

    SampleResult SampleCompress(const autil::StringView& hintData, size_t& sampleBlockCount, int64_t& useHintBlockCount,
                                int64_t& hintSaveSize);

private:
    static constexpr size_t DEFAULT_SAMPLE_BLOCK_COUNT = 2 * 1024;
    static constexpr float DEFAULT_SAMPLE_RATIO = 0.01f;

    const static uint64_t SLICE_LEN = 32 * 1024 * 1024;
    const static uint32_t SLICE_NUM = 32 * 1024;

private:
    typedef util::AlignedSliceArray<char> SliceArray;

private:
    size_t _trainHintBlockCount;
    std::shared_ptr<FileWriter> _hintWriter;
    std::shared_ptr<util::BufferCompressor> _hintCompressor;
    std::shared_ptr<util::CompressHintDataTrainer> _hintTrainer;
    std::vector<size_t> _hintMetaVec;
    int64_t _useHintSavedSize;
    SliceArray* _hintDataArray;
    size_t _hintDataLen;
    kmonitor::MetricsTags _normalTags;

    uint32_t _disableHintCounter;
    bool _alwaysAdaptiveCompress;
    bool _alwaysHintCompress;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<HintCompressDataDumper> HintCompressDataDumperPtr;

}} // namespace indexlib::file_system
