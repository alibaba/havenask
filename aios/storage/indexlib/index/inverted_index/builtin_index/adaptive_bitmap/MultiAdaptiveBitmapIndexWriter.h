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

#include "autil/Log.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/IndexDataWriter.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapIndexWriter.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::file_system {
class RelocatableFolder;
}

namespace indexlib::index {

class MultiAdaptiveBitmapIndexWriter
{
public:
    MultiAdaptiveBitmapIndexWriter(
        const std::shared_ptr<AdaptiveBitmapTrigger>& trigger,
        const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf,
        const std::shared_ptr<file_system::RelocatableFolder>& adaptiveDictFolder,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
        : _adaptiveDictFolder(adaptiveDictFolder)
        , _indexConfig(indexConf)
    {
        for (size_t i = 0; i < targetSegments.size(); i++) {
            auto adaptiveBitmapIndexWriter =
                std::make_shared<AdaptiveBitmapIndexWriter>(trigger, indexConf, adaptiveDictFolder);
            _adaptiveBitmapIndexWriters.push_back(adaptiveBitmapIndexWriter);
        }
    }
    virtual ~MultiAdaptiveBitmapIndexWriter() = default;

    void Init(std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources)
    {
        assert(indexOutputSegmentResources.size() == _adaptiveBitmapIndexWriters.size());
        for (size_t i = 0; i < indexOutputSegmentResources.size(); i++) {
            std::shared_ptr<IndexDataWriter> bitmapIndexDataWriter =
                indexOutputSegmentResources[i]->GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);

            _adaptiveBitmapIndexWriters[i]->Init(bitmapIndexDataWriter->dictWriter,
                                                 bitmapIndexDataWriter->postingWriter);
        }
    }

    void EndPosting()
    {
        std::shared_ptr<config::HighFrequencyVocabulary> vocabulary =
            config::HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
                _indexConfig->GetIndexName(), _indexConfig->GetInvertedIndexType(),
                _indexConfig->GetNullTermLiteralString(), _indexConfig->GetDictConfig(),
                _indexConfig->GetDictHashParams());
        assert(vocabulary);
        for (auto writer : _adaptiveBitmapIndexWriters) {
            auto& dictKeys = writer->GetAdaptiveDictKeys();
            for (auto key : dictKeys) {
                vocabulary->AddKey(key);
            }
        }

        auto status = vocabulary->DumpTerms(_adaptiveDictFolder);
        THROW_IF_STATUS_ERROR(status);
    }

    bool NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingMerger>& postingMerger)
    {
        return _adaptiveBitmapIndexWriters[0]->NeedAdaptiveBitmap(dictKey, postingMerger);
    }

    virtual void AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                            const std::shared_ptr<PostingIterator>& postingIt)
    {
        auto multiIter = std::dynamic_pointer_cast<MultiSegmentPostingIterator>(postingIt);
        for (size_t i = 0; i < multiIter->GetSegmentPostingCount(); i++) {
            segmentid_t segmentIdx;
            auto postingIter = multiIter->GetSegmentPostingIter(i, segmentIdx);
            _adaptiveBitmapIndexWriters[segmentIdx]->AddPosting(dictKey, termPayload, postingIter);
        }
    }

    int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(AdaptiveBitmapIndexWriter) * _adaptiveBitmapIndexWriters.size();
        size += docCount * sizeof(docid_t) * 2;
        size +=
            file_system::WriterOption::DEFAULT_BUFFER_SIZE * 2 * _adaptiveBitmapIndexWriters.size(); // data and dict
        return size;
    }

public:
    std::shared_ptr<AdaptiveBitmapIndexWriter> GetSingleAdaptiveBitmapWriter(uint32_t idx)
    {
        return _adaptiveBitmapIndexWriters[idx];
    }

private:
    std::vector<std::shared_ptr<AdaptiveBitmapIndexWriter>> _adaptiveBitmapIndexWriters;
    std::shared_ptr<file_system::RelocatableFolder> _adaptiveDictFolder;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
