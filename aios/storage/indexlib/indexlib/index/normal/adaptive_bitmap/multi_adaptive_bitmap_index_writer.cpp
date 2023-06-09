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
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"

#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace indexlib::config;
namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, MultiAdaptiveBitmapIndexWriter);

MultiAdaptiveBitmapIndexWriter::~MultiAdaptiveBitmapIndexWriter() {}

void MultiAdaptiveBitmapIndexWriter::EndPosting()
{
    std::shared_ptr<HighFrequencyVocabulary> vocabulary = HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
        mIndexConfig->GetIndexName(), mIndexConfig->GetInvertedIndexType(), mIndexConfig->GetNullTermLiteralString(),
        mIndexConfig->GetDictConfig(), mIndexConfig->GetDictHashParams());
    assert(vocabulary);
    for (auto writer : mAdaptiveBitmapIndexWriters) {
        auto& dictKeys = writer->GetAdaptiveDictKeys();
        for (auto key : dictKeys) {
            vocabulary->AddKey(key);
        }
    }

    auto status = vocabulary->DumpTerms(mAdaptiveDictFolder);
    THROW_IF_STATUS_ERROR(status);
}
}}} // namespace indexlib::index::legacy
