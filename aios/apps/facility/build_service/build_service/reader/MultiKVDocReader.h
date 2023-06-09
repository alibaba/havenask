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
#ifndef ISEARCH_BS_MULTIKVDOCREADER_H
#define ISEARCH_BS_MULTIKVDOCREADER_H

#include <assert.h>

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kv/doc_reader_base.h"

namespace build_service { namespace reader {

struct KVCheckPoint {
    int64_t itemIdx;
    int64_t segmentIdx;
    int64_t offsetInSegment;
};

static_assert(sizeof(KVCheckPoint) == 24, "size of KVCheckPoint is not equal to 24 byte");

class MultiKVDocReader : public RawDocumentReader
{
public:
    MultiKVDocReader();
    ~MultiKVDocReader();

private:
    MultiKVDocReader(const MultiKVDocReader&);
    MultiKVDocReader& operator=(const MultiKVDocReader&);

public:
    RawDocumentReader::ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                               DocInfo& docInfo) override;
    indexlib::document::RawDocumentParser* createRawDocumentParser(const ReaderInitParam& params) override;
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override
    {
        assert(false);
        return ERROR_EXCEPTION;
    }
    void doFillDocTags(document::RawDocument& rawDoc) override;

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;
    void updateCommitedCheckpoint(Checkpoint* checkpoint);
    void getCommitedCheckpoint(Checkpoint* checkpoint);
    void setAutoCommit(bool autoCommit) { _autoCommit = autoCommit; }

private:
    void reportRawDocSize(document::RawDocument& rawDoc);
    void fillTargetItems(const std::vector<std::pair<uint32_t, uint32_t>>& ranges, const proto::Range& range);
    bool switchPartitionShard(int64_t itemIdx, int64_t segmentIdx, int64_t offset);
    bool loadSrcSchema(const std::string& indexRootPath);
    void selectShardItems(uint32_t myFrom, uint32_t myTo, versionid_t versionId,
                          std::vector<std::pair<std::pair<uint32_t, uint32_t>, std::string>>& rangePath);

private:
    struct KVReaderInitParam {
        KVReaderInitParam(std::string indexRootPath, versionid_t indexVersion, uint32_t shardIdx,
                          std::pair<uint32_t, uint32_t> indexRange)
            : indexRootPath(indexRootPath)
            , indexVersion(indexVersion)
            , shardIdx(shardIdx)
            , indexRange(indexRange)
        {
        }

        std::string indexRootPath;
        versionid_t indexVersion = INVALID_VERSION;
        uint32_t shardIdx = 0;
        std::pair<uint32_t, uint32_t> indexRange;
    };

private:
    bool _autoCommit = true;
    KVCheckPoint _currentProgress;
    KVCheckPoint _committedCheckPoint;
    int64_t _checkpointDocCounter = 0;

    int64_t _timestamp = 0;
    std::string _ttlFieldName;
    indexlib::index::DocReaderBasePtr _docReader;
    std::vector<KVReaderInitParam> _totalItems;
    indexlib::config::IndexPartitionOptions _options;
    indexlib::config::IndexPartitionSchemaPtr _srcSchema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiKVDocReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_MULTIKVDOCREADER_H
