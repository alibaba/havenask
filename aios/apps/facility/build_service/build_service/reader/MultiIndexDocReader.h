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
#ifndef ISEARCH_BS_INDEXDOCFILTERREADER_H
#define ISEARCH_BS_INDEXDOCFILTERREADER_H

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/SingleIndexDocReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class MultiIndexDocReader : public RawDocumentReader
{
public:
    MultiIndexDocReader();
    ~MultiIndexDocReader();

private:
    MultiIndexDocReader(const MultiIndexDocReader&);
    MultiIndexDocReader& operator=(const MultiIndexDocReader&);

protected:
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

private:
    void reportRawDocSize(document::RawDocument& rawDoc);
    void fillTargetItems(const std::vector<std::pair<uint32_t, uint32_t>>& ranges, const proto::Range& range);
    bool switchPartition(int32_t itemIdx, int64_t offset);
    int64_t getLocator(int64_t itemIdx, int64_t docOffset) const;

    struct LastDocInfo {
        int64_t docId;
        versionid_t indexVersion;
        std::pair<uint32_t, uint32_t> indexRange;
    };

    static const int32_t OFFSET_LOCATOR_BIT_COUNT;

private:
    SingleIndexDocReaderPtr _indexDocReader;
    std::vector<SingleIndexDocReader::ReaderInitParam> _totalItems;
    int32_t _currentItemIdx;
    int64_t _nextDocId;
    LastDocInfo _lastDocInfo;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiIndexDocReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_INDEXDOCFILTERREADER_H
