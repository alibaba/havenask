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

#include <memory>
#include <string>

#include "sql/resource/MessageWriter.h"
#include "suez/sdk/RemoteTableWriter.h"

namespace suez {
struct MessageWriteParam;
} // namespace suez

namespace sql {

class TableMessageWriter : public MessageWriter {
public:
    TableMessageWriter(const std::string &tableName, suez::RemoteTableWriterPtr remoteTableWriter);
    ~TableMessageWriter();
    TableMessageWriter(const TableMessageWriter &) = delete;
    TableMessageWriter &operator=(const TableMessageWriter &) = delete;

public:
    void write(suez::MessageWriteParam param) override;

private:
    std::string _tableName;
    suez::RemoteTableWriterPtr _remoteTableWriter;

private:
    static const std::string RAW_DOCUMENT_FORMAT_SWIFT;
};

typedef std::shared_ptr<TableMessageWriter> TableMessageWriterPtr;

} // namespace sql
