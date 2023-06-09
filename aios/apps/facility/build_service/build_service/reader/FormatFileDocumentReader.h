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
#ifndef ISEARCH_BS_FORMATFILEDOCUMENTREADER_H
#define ISEARCH_BS_FORMATFILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/reader/FileReaderBase.h"
#include "build_service/reader/Separator.h"
#include "build_service/util/Log.h"
#include "fslib/fslib.h"

namespace build_service { namespace reader {

class FormatFileDocumentReader : public FileDocumentReader
{
public:
    static const uint32_t READER_DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

public:
    FormatFileDocumentReader(const std::string& docPrefix, const std::string& docSuffix,
                             uint32_t bufferSize = READER_DEFAULT_BUFFER_SIZE);
    virtual ~FormatFileDocumentReader();

private:
    FormatFileDocumentReader(const FormatFileDocumentReader&);
    FormatFileDocumentReader& operator=(const FormatFileDocumentReader&);

protected:
    bool doRead(std::string& docStr) override;

private:
    bool findSeparator(const Separator& sep, std::string& docStr);

private:
    Separator _docPrefix;
    Separator _docSuffix;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FormatFileDocumentReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_FORMATFILEDOCUMENTREADER_H
