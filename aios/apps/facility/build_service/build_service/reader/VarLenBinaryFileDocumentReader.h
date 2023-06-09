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
#ifndef ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H
#define ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/reader/FileDocumentReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class VarLenBinaryFileDocumentReader : public FileDocumentReader
{
public:
    static const uint32_t READER_DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

public:
    VarLenBinaryFileDocumentReader(uint32_t bufferSize = READER_DEFAULT_BUFFER_SIZE);
    ~VarLenBinaryFileDocumentReader();

private:
    VarLenBinaryFileDocumentReader(const VarLenBinaryFileDocumentReader&);
    VarLenBinaryFileDocumentReader& operator=(const VarLenBinaryFileDocumentReader&);

protected:
    bool doRead(std::string& docStr) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(VarLenBinaryFileDocumentReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_VARLENBINARYFILEDOCUMENTREADER_H
