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
#ifndef ISEARCH_BS_MULTIFILEDOCUMENTREADER_H
#define ISEARCH_BS_MULTIFILEDOCUMENTREADER_H

#include "autil/cipher/AESCipherCommon.h"
#include "build_service/reader/CollectResult.h"
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/util/Log.h"

namespace indexlib { namespace util {
class Metric;
class MetricProvider;
typedef std::shared_ptr<Metric> MetricPtr;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace reader {

class MultiFileDocumentReader
{
public:
    MultiFileDocumentReader(const CollectResults& fileList, const indexlib::util::MetricProviderPtr& metricProvider);
    virtual ~MultiFileDocumentReader() {}

public:
    // init for format document by seperators
    bool initForFormatDocument(const std::string& docPrefix, const std::string& docSuffix);

    // init for format document by seperators, and file is encrypted with aes cipher
    bool initForCipherFormatDocument(autil::cipher::CipherOption option,
                                     const std::string& docPrefix, const std::string& docSuffix);

    // init for binary document
    bool initForFixLenBinaryDocument(size_t length);
    bool initForVarLenBinaryDocument();

    bool read(std::string& docStr, int64_t& locator);
    bool isEof();
    bool seek(int64_t locator);
    double getProcessProgress() const;

private:
    bool skipToFileId(int64_t fileId);
    bool skipToNextFile();
    void createDocumentReader();
    bool innerInit();

protected:
    // virtual for mock
    virtual bool calculateTotalFileSize();

public:
    void appendLocator(int64_t& locator);
    static void transLocatorToFileOffset(int64_t locator, int64_t& fileIndex, int64_t& offset);
    static void transFileOffsetToLocator(int64_t fileIndex, int64_t offset, int64_t& locator);

private:
    static const int32_t FILE_INDEX_LOCATOR_BIT_COUNT;
    static const uint64_t MAX_LOCATOR_FILE_INDEX;
    static const int32_t OFFSET_LOCATOR_BIT_COUNT;
    static const uint64_t MAX_LOCATOR_OFFSET;

private:
    int32_t _fileCursor;
    CollectResults _fileList;
    FileDocumentReaderPtr _fileDocumentReaderPtr;
    int64_t _processedFileSize;
    int64_t _totalFileSize;
    indexlib::util::MetricPtr _progressMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiFileDocumentReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_MULTIFILEDOCUMENTREADER_H
