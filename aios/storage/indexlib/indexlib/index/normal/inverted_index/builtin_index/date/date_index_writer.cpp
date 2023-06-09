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
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_writer.h"

#include "indexlib/config/date_index_config.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_in_mem_segment_reader.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DateIndexWriter);

DateIndexWriter::DateIndexWriter(size_t lastSegmentDistinctTermCount, const config::IndexPartitionOptions& options)
    : NormalIndexWriter(INVALID_SEGMENTID, lastSegmentDistinctTermCount, options)
    , mTimeRangeInfo(new TimeRangeInfo)
{
}

DateIndexWriter::~DateIndexWriter() {}

void DateIndexWriter::Init(const config::IndexConfigPtr& indexConfig, BuildResourceMetrics* buildResourceMetrics)
{
    NormalIndexWriter::Init(indexConfig, buildResourceMetrics);
    DateIndexConfigPtr config = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    DateLevelFormat::Granularity granularity = config->GetBuildGranularity();
    mGranularityLevel = DateLevelFormat::GetGranularityLevel(granularity);
    DateIndexConfigPtr dateIndexConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, _indexConfig);
    mFormat = dateIndexConfig->GetDateLevelFormat();
}

void DateIndexWriter::AddField(const Field* field)
{
    if (!field) {
        return;
    }

    Field::FieldTag tag = field->GetFieldTag();
    if (tag == Field::FieldTag::NULL_FIELD) {
        NormalIndexWriter::AddField(field);
        return;
    }

    auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return;
    }

    auto iterField = tokenizeField->Begin();

    const Section* section = *iterField;
    if (section == NULL || section->GetTokenCount() <= 0) {
        return;
    }

    const Token* token = section->GetToken(0);
    DateTerm term(token->GetHashKey());
    assert(term.GetLevel() == mGranularityLevel);
    term.SetLevelType(0);
    mTimeRangeInfo->Update(term.GetKey());
    NormalIndexWriter::AddField(field);
}

void DateIndexWriter::Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool)
{
    NormalIndexWriter::Dump(dir, dumpPool);
    file_system::DirectoryPtr indexDirectory = dir->GetDirectory(_indexConfig->GetIndexName(), true);
    auto status = mTimeRangeInfo->Store(indexDirectory->GetIDirectory());
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file load failed. [%s]",
                             indexDirectory->GetPhysicalPath("").c_str());
    }
}

std::shared_ptr<IndexSegmentReader> DateIndexWriter::CreateInMemReader()
{
    InMemNormalIndexSegmentReaderPtr indexSegmentReader =
        DYNAMIC_POINTER_CAST(InMemNormalIndexSegmentReader, NormalIndexWriter::CreateInMemReader());
    return DateInMemSegmentReaderPtr(new DateInMemSegmentReader(indexSegmentReader, mFormat, mTimeRangeInfo));
}
}} // namespace indexlib::index
