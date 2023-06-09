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
#include "indexlib/index/inverted_index/builtin_index/date/DateMemIndexer.h"

#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/Token.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateLeafMemReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DateMemIndexer);

DateMemIndexer::DateMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam,
                               const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : InvertedMemIndexer(indexerParam, metrics)
    , _timeRangeInfo(new TimeRangeInfo)
{
}

DateMemIndexer::~DateMemIndexer() {}

Status DateMemIndexer::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                            indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto status = InvertedMemIndexer::Init(indexConfig, docInfoExtractorFactory);
    RETURN_IF_STATUS_ERROR(status, "inverted mem indexer init fail, indexName[%s]", GetIndexName().c_str());
    auto config = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    assert(config);
    config::DateLevelFormat::Granularity granularity = config->GetBuildGranularity();
    _granularityLevel = config::DateLevelFormat::GetGranularityLevel(granularity);
    _format = config->GetDateLevelFormat();
    return Status::OK();
}

Status DateMemIndexer::AddField(const document::Field* field)
{
    if (!field) {
        return Status::OK();
    }

    document::Field::FieldTag tag = field->GetFieldTag();
    if (tag == document::Field::FieldTag::NULL_FIELD) {
        return InvertedMemIndexer::AddField(field);
    }

    auto tokenizeField = dynamic_cast<const document::IndexTokenizeField*>(field);
    if (!tokenizeField) {
        AUTIL_LOG(ERROR, "fieldTag [%d] is not IndexTokenizeField", static_cast<int8_t>(field->GetFieldTag()));
        return Status::ConfigError("field tag is not IndexTokenizeField");
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return Status::OK();
    }

    auto iterField = tokenizeField->Begin();

    const document::Section* section = *iterField;
    if (section == NULL || section->GetTokenCount() <= 0) {
        return Status::OK();
    }

    const document::Token* token = section->GetToken(0);
    DateTerm term(token->GetHashKey());
    assert(term.GetLevel() == _granularityLevel);
    term.SetLevelType(0);
    _timeRangeInfo->Update(term.GetKey());
    return InvertedMemIndexer::AddField(field);
}

std::shared_ptr<IndexSegmentReader> DateMemIndexer::CreateInMemReader()
{
    auto leafMemReader = std::dynamic_pointer_cast<InvertedLeafMemReader>(InvertedMemIndexer::CreateInMemReader());
    assert(leafMemReader);
    return std::make_shared<DateLeafMemReader>(leafMemReader, _format, _timeRangeInfo);
}

Status DateMemIndexer::DoDump(autil::mem_pool::PoolBase* dumpPool,
                              const std::shared_ptr<file_system::Directory>& indexDirectory,
                              const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    auto status = InvertedMemIndexer::DoDump(dumpPool, indexDirectory, dumpParams);
    RETURN_IF_STATUS_ERROR(status, "inverted mem indexer dump fail, indexName[%s]", GetIndexName().c_str());
    auto subDir = indexDirectory->GetDirectory(GetIndexName(), false);
    assert(subDir);
    return _timeRangeInfo->Store(subDir->GetIDirectory());
}

} // namespace indexlib::index
