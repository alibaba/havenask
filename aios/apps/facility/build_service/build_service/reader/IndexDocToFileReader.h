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
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"

namespace build_service::io {
class FileOutput;
}

namespace build_service { namespace reader {

/*
  {
   "output_path" : "dfs://"
  }
 */
class IndexDocReader;

class IndexDocToFileReader : public RawDocumentReader
{
public:
    IndexDocToFileReader();
    ~IndexDocToFileReader();

    IndexDocToFileReader(const IndexDocToFileReader&) = delete;
    IndexDocToFileReader& operator=(const IndexDocToFileReader&) = delete;
    IndexDocToFileReader(IndexDocToFileReader&&) = delete;
    IndexDocToFileReader& operator=(IndexDocToFileReader&&) = delete;

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;
    RawDocumentReader::ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                               DocInfo& docInfo) override;

    std::string TEST_getOutputFileName() const;

protected:
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override
    {
        assert(false);
        return ERROR_EXCEPTION;
    }
    void doFillDocTags(document::RawDocument& rawDoc) override { assert(false); }

    indexlib::document::RawDocumentParser* createRawDocumentParser(const ReaderInitParam& params) override;

private:
    bool writerRecover(std::string& inheritFileName, size_t& fileLength);
    void waitAllDocCommit();
    void setAndSaveCheckpoint(size_t currentOutputFileLen, Checkpoint* checkpoint);
    bool resolveCheckpoint(const Checkpoint& checkpoint, std::string& fileName, size_t& fileLength,
                           Checkpoint& readerCkpt);
    bool finalMove(size_t outputFileLen, Checkpoint* checkpoint);
    std::string serializeHa3RawDoc(document::RawDocument& rawDoc);
    std::string serializeRawDoc(document::RawDocument& rawDoc);
    std::string serializeDocCheckStr(document::RawDocument& rawDoc);
    bool initOutput();

private:
    const std::string OUTPUT_FILE_NAME_PREIFX = "output.";
    const std::string TMP_SUFFIX = ".tmp";
    const size_t DEFAULT_MAX_FILE_LENGTH = 4UL * 1024 * 1024 * 1024; // 4G
    const uint64_t DEFAULT_SYNC_INTERVAL = 100;                      // 100s

private:
    std::unique_ptr<IndexDocReader> _docReader;
    std::unique_ptr<io::FileOutput> _output;
    std::string _outputPath;
    std::string _currentFileName;
    std::string _fileNameLen;
    Checkpoint _checkpoint;
    ReaderInitParam _params;
    int64_t _lastCommitTime = -1;
    uint64_t _syncInterval = DEFAULT_SYNC_INTERVAL;
    size_t _maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    bool _docCheck = false;
    std::string _pkField;

private:
    indexlib::util::MetricPtr _writeLatencyMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexDocToFileReader);

}} // namespace build_service::reader
