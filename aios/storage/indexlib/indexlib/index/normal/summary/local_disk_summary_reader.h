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
#ifndef __INDEXLIB_LOCAL_DISK_SUMMARY_READER_H
#define __INDEXLIB_LOCAL_DISK_SUMMARY_READER_H

#include <memory>
#include <string>

#include "fslib/fs/FileSystem.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/building_summary_reader.h"
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class LocalDiskSummaryReader : public SummaryReader
{
public:
    typedef std::vector<std::pair<fieldid_t, AttributeReaderPtr>> AttributeReaders;
    typedef std::vector<std::pair<fieldid_t, PackAttributeReaderPtr>> PackAttributeReaders;

public:
    LocalDiskSummaryReader(const config::SummarySchemaPtr& summarySchema, summarygroupid_t summaryGroupId);
    ~LocalDiskSummaryReader();

    DECLARE_SUMMARY_READER_IDENTIFIER(local_disk);

public:
    bool Open(const index_base::PartitionDataPtr& partitionData,
              const std::shared_ptr<PrimaryKeyIndexReader>& pkIndexReader, const SummaryReader* hintReader) override;

    future_lite::coro::Lazy<index::ErrorCodeVec> GetDocumentAsync(const std::vector<docid_t>& docIds,
                                                                  autil::mem_pool::Pool* sessionPool,
                                                                  file_system::ReadOption readOption,
                                                                  const SearchSummaryDocVec* docs) const noexcept;

    bool GetDocument(docid_t docId, document::SearchSummaryDocument* summaryDoc) const override;

    void AddAttrReader(fieldid_t fieldId, const AttributeReaderPtr& attrReader) override;
    void AddPackAttrReader(fieldid_t fieldId, const PackAttributeReaderPtr& attrReader) override;

private:
    bool LoadSegment(const std::string& segPath);
    void LoadSegmentInfo(const std::string& segPath, index_base::SegmentInfo& segInfo);
    bool GetDocumentFromSummary(docid_t docId, document::SearchSummaryDocument* summaryDoc) const;
    bool GetDocumentFromAttributes(docid_t docId, document::SearchSummaryDocument* summaryDoc) const;
    bool LoadSegmentReader(const index_base::SegmentData& segmentData);

    void InitBuildingSummaryReader(const index_base::SegmentIteratorPtr& segIter);

    bool SetSummaryDocField(document::SearchSummaryDocument* summaryDoc, fieldid_t fieldId,
                            const std::string& value) const;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    GetDocumentFromSummaryAsync(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                                file_system::ReadOption readOption, const SearchSummaryDocVec* docs) const noexcept;
    future_lite::coro::Lazy<std::vector<future_lite::Try<index::ErrorCodeVec>>>
    GetBuiltSegmentTasks(const std::vector<docid_t>& docIds, autil::mem_pool::Pool* sessionPool,
                         file_system::ReadOption readOption, const SearchSummaryDocVec* docs) const noexcept;

private:
    AttributeReaders mAttrReaders;
    PackAttributeReaders mPackAttrReaders;
    std::vector<LocalDiskSummarySegmentReaderPtr> mSegmentReaders;
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    std::vector<uint64_t> mSegmentDocCount;

    BuildingSummaryReaderPtr mBuildingSummaryReader;
    docid_t mBuildingBaseDocId;

    std::vector<segmentid_t> mReaderSegmentIds;

private:
    friend class LocalDiskSummaryReaderTest;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(LocalDiskSummaryReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_LOCAL_DISK_SUMMARY_READER_H
