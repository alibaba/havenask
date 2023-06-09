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
#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/index/summary/Types.h"

namespace indexlibv2::document {
class RawDocument;
}
namespace indexlib::index {
class PostingExecutor;
class IndexTermInfo;
class IndexQueryCondition;
} // namespace indexlib::index
namespace indexlibv2::index {
class SummaryReader;
class DeletionMapIndexReader;
class AttributeIteratorBase;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::table {
class NormalTabletReader;

class NormalTabletDocIterator : public framework::ITabletDocIterator
{
public:
    NormalTabletDocIterator();
    ~NormalTabletDocIterator();

public:
    Status Init(const std::shared_ptr<framework::TabletData>& tabletData,
                std::pair<uint32_t /*0-99*/, uint32_t /*0-99*/> rangeInRatio,
                const std::shared_ptr<indexlibv2::framework::MetricsManager>& metricsManager,
                const std::map<std::string, std::string>& params) override;
    Status Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                document::IDocument::DocInfo* docInfo) override;
    bool HasNext() const override;
    Status Seek(const std::string& checkpoint) override;

private:
    Status SeekByDocId(docid_t docId);
    Status InitFieldReaders(const std::string& requiredFieldsStr);

    Status InitPostingExecutor(const std::string& userDefineIndexParamStr);
    std::shared_ptr<indexlib::index::PostingExecutor>
    CreateUserDefinePostingExecutor(const std::vector<indexlib::index::IndexQueryCondition>& userDefineParam);
    std::shared_ptr<indexlib::index::PostingExecutor>
    CreateSinglePostingExecutor(const indexlib::index::IndexQueryCondition& indexQueryCondition);
    std::shared_ptr<indexlib::index::PostingExecutor>
    CreateTermPostingExecutor(const indexlib::index::IndexTermInfo& termInfo);

private:
    std::shared_ptr<NormalTabletReader> _tabletReader;
    std::map<std::string, std::unique_ptr<index::AttributeIteratorBase>> _attrFieldIters;
    std::shared_ptr<index::SummaryReader> _summaryReader;
    std::vector<std::pair<std::string, index::summaryfieldid_t>> _summaryFields;
    std::map<std::string, std::string> _params;
    size_t _summaryCount = 0;
    autil::mem_pool::Pool _invertedIndexPool;
    std::shared_ptr<index::DeletionMapIndexReader> _deletionMapReader;
    std::pair<docid_t, docid_t> _docRange = {INVALID_DOCID, INVALID_DOCID}; // [from, to)
    std::shared_ptr<indexlib::index::PostingExecutor> _postingExecutor;
    docid_t _currentDocId = INVALID_DOCID;
    bool _tryBestExport = false;
    const static std::string USER_REQUIRED_FIELDS;
    const static std::string USER_DEFINE_INDEX_PARAM;
    const static std::string BUILDIN_KEEP_DELETED_DOC;
    const static std::string BUILDIN_DOCID_RANGE;
    const static uint32_t INVALID_RATIO;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
