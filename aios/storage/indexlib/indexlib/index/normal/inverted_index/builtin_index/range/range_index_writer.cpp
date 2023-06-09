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
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_writer.h"

#include "indexlib/config/range_index_config.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_in_mem_segment_reader.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, RangeIndexWriter);

RangeIndexWriter::RangeIndexWriter(const std::string& indexName,
                                   std::shared_ptr<framework::SegmentMetrics> segmentMetrics,
                                   const config::IndexPartitionOptions& options)
    : mBasePos(0)
    , mRangeInfo(new RangeInfo)
{
    size_t lastBottomLevelTermCount = 0;
    size_t lastHighLevelTermCount = 0;
    GetDistinctTermCount(segmentMetrics, indexName, lastHighLevelTermCount, lastBottomLevelTermCount);
    mBottomLevelWriter.reset(new NormalIndexWriter(INVALID_SEGMENTID, lastBottomLevelTermCount, options));
    mHighLevelWriter.reset(new NormalIndexWriter(INVALID_SEGMENTID, lastHighLevelTermCount, options));
}

RangeIndexWriter::~RangeIndexWriter() {}

void RangeIndexWriter::Init(const IndexConfigPtr& indexConfig, BuildResourceMetrics* buildResourceMetrics)
{
    _indexConfig = indexConfig;
    // TODO: Maybe support multi sharding for range index.
    assert(_indexConfig->GetShardingType() != config::IndexConfig::IST_NEED_SHARDING);
    string indexName = indexConfig->GetIndexName();
    RangeIndexConfigPtr rangeIndexConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    IndexConfigPtr bottomLevelConfig = rangeIndexConfig->GetBottomLevelIndexConfig();
    mBottomLevelWriter->Init(bottomLevelConfig, buildResourceMetrics);

    IndexConfigPtr highLevelConfig = rangeIndexConfig->GetHighLevelIndexConfig();
    mHighLevelWriter->Init(highLevelConfig, buildResourceMetrics);
}

size_t RangeIndexWriter::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                            const index_base::PartitionSegmentIteratorPtr& segIter)
{
    return mBottomLevelWriter->EstimateInitMemUse(indexConfig, segIter) +
           mHighLevelWriter->EstimateInitMemUse(indexConfig, segIter);
}

void RangeIndexWriter::FillDistinctTermCount(std::shared_ptr<indexlib::framework::SegmentMetrics> mSegmentMetrics)
{
    mBottomLevelWriter->FillDistinctTermCount(mSegmentMetrics);
    mHighLevelWriter->FillDistinctTermCount(mSegmentMetrics);
}

IndexSegmentReaderPtr RangeIndexWriter::CreateInMemReader()
{
    InMemNormalIndexSegmentReaderPtr bottomReader =
        DYNAMIC_POINTER_CAST(InMemNormalIndexSegmentReader, mBottomLevelWriter->CreateInMemReader());
    InMemNormalIndexSegmentReaderPtr highReader =
        DYNAMIC_POINTER_CAST(InMemNormalIndexSegmentReader, mHighLevelWriter->CreateInMemReader());
    return IndexSegmentReaderPtr(new RangeInMemSegmentReader(bottomReader, highReader, mRangeInfo));
}

void RangeIndexWriter::AddField(const document::Field* field)
{
    auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return;
    }

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); iterField++) {
        const Section* section = *iterField;
        if (section == NULL || section->GetTokenCount() <= 0) {
            continue;
        }

        const Token* token = section->GetToken(0);
        mRangeInfo->Update(token->GetHashKey());

        pos_t tokenBasePos = mBasePos + token->GetPosIncrement();
        fieldid_t fieldId = tokenizeField->GetFieldId();
        mBottomLevelWriter->AddToken(token, fieldId, tokenBasePos);
        for (size_t i = 1; i < section->GetTokenCount(); i++) {
            const Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            mHighLevelWriter->AddToken(token, fieldId, tokenBasePos);
        }
        mBasePos += section->GetLength();
    }
}

void RangeIndexWriter::EndDocument(const IndexDocument& indexDocument)
{
    mBottomLevelWriter->EndDocument(indexDocument);
    mHighLevelWriter->EndDocument(indexDocument);
    mBasePos = 0;
}

void RangeIndexWriter::EndSegment()
{
    mBottomLevelWriter->EndSegment();
    mHighLevelWriter->EndSegment();
}

void RangeIndexWriter::Dump(const file_system::DirectoryPtr& dir, mem_pool::PoolBase* dumpPool)
{
    file_system::DirectoryPtr indexDirectory = dir->MakeDirectory(_indexConfig->GetIndexName());
    mBottomLevelWriter->Dump(indexDirectory, dumpPool);
    mHighLevelWriter->Dump(indexDirectory, dumpPool);
    auto status = mRangeInfo->Store(indexDirectory->GetIDirectory());
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file store failed. [%s]",
                             indexDirectory->GetPhysicalPath("").c_str());
    }
}

void RangeIndexWriter::SetTemperatureLayer(const std::string& layer)
{
    IndexWriter::SetTemperatureLayer(layer);
    mBottomLevelWriter->SetTemperatureLayer(layer);
    mHighLevelWriter->SetTemperatureLayer(layer);
}

void RangeIndexWriter::GetDistinctTermCount(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics,
                                            const string& indexName, size_t& highLevelTermCount,
                                            size_t& bottomLevelTermCount) const
{
    auto highLevelIndexName = RangeIndexConfig::GetHighLevelIndexName(indexName);
    auto bottomLevelIndexName = RangeIndexConfig::GetBottomLevelIndexName(indexName);
    highLevelTermCount = segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(highLevelIndexName) : 0;
    bottomLevelTermCount = segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(bottomLevelIndexName) : 0;
    if (highLevelTermCount > 0 && bottomLevelTermCount > 0) {
        return;
    }

    size_t totalTermCount = segmentMetrics != nullptr ? segmentMetrics->GetDistinctTermCount(indexName) : 0;
    if (totalTermCount == 0) {
        totalTermCount = HASHMAP_INIT_SIZE * 10;
    }
    highLevelTermCount = totalTermCount * (1 - DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO);
    bottomLevelTermCount = totalTermCount * DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO;
}

}} // namespace indexlib::index
