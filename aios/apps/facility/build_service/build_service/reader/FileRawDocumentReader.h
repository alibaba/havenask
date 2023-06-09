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
#ifndef ISEARCH_BS_FILERAWDOCUMENTREADER_H
#define ISEARCH_BS_FILERAWDOCUMENTREADER_H

#include "autil/cipher/AESCipherCommon.h"
#include "build_service/common_define.h"
#include "build_service/reader/CollectResult.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class MultiFileDocumentReader;

class FileRawDocumentReader : public RawDocumentReader
{
public:
    FileRawDocumentReader();
    virtual ~FileRawDocumentReader();

private:
    FileRawDocumentReader(const FileRawDocumentReader&);
    FileRawDocumentReader& operator=(const FileRawDocumentReader&);

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;

protected:
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override;

protected:
    virtual MultiFileDocumentReader*
    createMultiFileDocumentReader(const CollectResults& collectResult, const KeyValueMap& kvMap,
                                  const indexlib::util::MetricProviderPtr& metricProvider);
    void doFillDocTags(document::RawDocument& rawDoc) override;

private:
    bool rewriteFilePathForDcacheIfNeeded(const KeyValueMap& kv, CollectResults& fileList);
    bool extractCipherOption(const KeyValueMap& kvMap, autil::cipher::CipherOption& option, bool& hasError) const;

protected:
    indexlib::util::MetricPtr _progressMetric;
    MultiFileDocumentReader* _documentReader;
    int64_t _lastOffset;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::reader

#endif // ISEARCH_BS_FILERAWDOCUMENTREADER_H
