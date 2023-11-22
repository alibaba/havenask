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
#include "indexlib/index/normal/summary/summary_reader.h"

namespace indexlib { namespace index {
class PrimaryKeyIndexReader;
}} // namespace indexlib::index
namespace indexlibv2::index {
class SummaryReader;
class AttributeReader;

class TabletSummaryReader final : public indexlib::index::SummaryReader
{
public:
    TabletSummaryReader(indexlibv2::index::SummaryReader* summaryReader,
                        const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& pkIndexReader);
    ~TabletSummaryReader() = default;

public:
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t>& docIds, const indexlib::index::SummaryGroupIdVec& groupVec,
                autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                const indexlib::index::SearchSummaryDocVec* docs) const noexcept override;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocument(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                indexlib::file_system::ReadOption option,
                const indexlib::index::SearchSummaryDocVec* docs) const noexcept override;

    bool GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const override;

    bool GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc,
                     const indexlib::index::SummaryGroupIdVec& groupVec) const override;

    void AddAttrReader(fieldid_t fieldId, const std::shared_ptr<indexlib::index::AttributeReader>& attrReader) override;
    void AddAttrReader(fieldid_t fieldId, AttributeReader* attrReader);
    void SetPrimaryKeyReader(indexlib::index::PrimaryKeyIndexReader* pkIndexReader);

    static std::string Identifier() { return std::string("summary.reader.tablet"); }
    std::string GetIdentifier() const override { return Identifier(); }

private:
    indexlibv2::index::SummaryReader* _summaryReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
