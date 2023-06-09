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
#include "ha3/sql/resource/TableMessageWriter.h"
#include "autil/result/Errors.h"

using namespace std;
using namespace autil::result;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, TableMessageWriter);

const std::string TableMessageWriter::RAW_DOCUMENT_FORMAT_SWIFT = "swift_field_filter";

TableMessageWriter::TableMessageWriter(const std::string &tableName,
                                       suez::RemoteTableWriterPtr remoteTableWriter)
    : _tableName(tableName)
    , _remoteTableWriter(remoteTableWriter)
    {
        setWriterName(_tableName);
    }

TableMessageWriter::~TableMessageWriter() {
}

void TableMessageWriter::write(suez::MessageWriteParam param)
{
    if (_remoteTableWriter == nullptr) {
        param.cb(RuntimeError::make("remote table message writer is empty!"));
        return;
    }
    _remoteTableWriter->write(_tableName, RAW_DOCUMENT_FORMAT_SWIFT, std::move(param));
}

} //end namespace sql
} //end namespace isearch
