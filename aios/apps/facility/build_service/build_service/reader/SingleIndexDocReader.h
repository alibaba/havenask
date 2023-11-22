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

#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/reader/IndexQueryCondition.h"
#include "build_service/util/Log.h"
#include "indexlib/index/inverted_index/PostingExecutor.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/raw_document_field_extractor.h"

namespace build_service { namespace reader {

class SingleIndexDocReader
{
public:
    struct ReaderInitParam {
        ReaderInitParam(std::string indexRootPath, versionid_t indexVersion, bool preferSourceIndex,
                        bool ignoreReadError, std::pair<uint32_t, uint32_t> readRange,
                        std::pair<uint32_t, uint32_t> indexRange, int64_t blockCacheSize, int64_t cacheBlockSize,
                        std::vector<std::string> requiredFields, std::vector<IndexQueryCondition> userDefineIndexParam)
            : indexRootPath(indexRootPath)
            , indexVersion(indexVersion)
            , preferSourceIndex(preferSourceIndex)
            , ignoreReadError(ignoreReadError)
            , readRange(readRange)
            , indexRange(indexRange)
            , blockCacheSize(blockCacheSize)
            , cacheBlockSize(cacheBlockSize)
            , requiredFields(requiredFields)
            , userDefineIndexParam(userDefineIndexParam)
        {
        }
        ReaderInitParam() {}
        std::string indexRootPath;
        versionid_t indexVersion = indexlib::INVALID_VERSIONID;
        bool preferSourceIndex = true;
        bool ignoreReadError = false;
        std::pair<uint32_t, uint32_t> readRange;
        std::pair<uint32_t, uint32_t> indexRange;
        int64_t blockCacheSize = 20 * 1024 * 1024; // 20MB
        int64_t cacheBlockSize = 4 * 1024;         // 4k
        std::vector<std::string> requiredFields;   // default is empty, means get all fields
        std::vector<IndexQueryCondition> userDefineIndexParam;
    };

public:
    SingleIndexDocReader();
    ~SingleIndexDocReader();

private:
    SingleIndexDocReader(const SingleIndexDocReader&);
    SingleIndexDocReader& operator=(const SingleIndexDocReader&);

public:
    bool init(const ReaderInitParam& param, int64_t offset);
    bool read(document::RawDocument& doc);
    bool seek(int64_t locator);
    int64_t getCurrentDocid() const { return _currentDocId; }
    bool isEof() const { return _rawDocExtractor == nullptr; }

private:
    std::pair<int64_t, int64_t> splitDocIdRange(size_t totalDocCount, std::pair<uint32_t, uint32_t> readRange,
                                                std::pair<uint32_t, uint32_t> indexRange) const;

    bool initPostingExecutor(const std::pair<int64_t, int64_t>& targetRange,
                             const std::vector<IndexQueryCondition>& userDefineParam);

    std::shared_ptr<indexlib::index::PostingExecutor>
    createUserDefinePostingExecutor(const std::vector<IndexQueryCondition>& userDefineParam) const;

    std::shared_ptr<indexlib::index::PostingExecutor>
    createSinglePostingExecutor(const indexlib::partition::IndexPartitionReaderPtr& reader,
                                const IndexQueryCondition& indexQueryCondition) const;

    std::shared_ptr<indexlib::index::PostingExecutor>
    createTermPostingExecutor(const indexlib::partition::IndexPartitionReaderPtr& reader,
                              const IndexTermInfo& termInfo) const;

private:
    indexlib::partition::IndexPartitionPtr _indexPart;
    indexlib::partition::RawDocumentFieldExtractorPtr _rawDocExtractor;
    std::shared_ptr<indexlib::index::PostingExecutor> _postingExecutor;
    std::pair<int64_t, int64_t> _targetRange = {INVALID_DOCID, INVALID_DOCID};
    int64_t _currentDocId = INVALID_DOCID;
    bool _ignoreReadError = false;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleIndexDocReader);

}} // namespace build_service::reader
