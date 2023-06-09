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

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"

namespace matchdoc {

using flatbuffers::Offset;
using flatbuffers::Vector;

class MatchDocFormatter {

public:

    static flatbuffers::Offset<MatchRecords> toFBResultRecordsByColumns(
            const std::string &tableName,
            const std::vector<std::string> &fieldNameVec,
            const matchdoc::ReferenceVector &refs,
            const std::vector<matchdoc::MatchDoc> &docs,
            const std::vector<Offset<flatbuffers::String>> &tracerOffsets,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<MatchRecords> toFBResultRecordsByColumns(
            const std::string &tableName,
            const std::vector<std::string> &fieldNameVec,
            const matchdoc::ReferenceVector &refs,
            const std::vector<matchdoc::MatchDoc> &docs,
            flatbuffers::FlatBufferBuilder &fbb,
            std::string &errMsg);

    static flatbuffers::Offset<MatchRecords> toFBResultRecordsByColumns(
            const std::string &tableName,
            const std::vector<std::string> &fieldNameVec,
            const matchdoc::ReferenceVector &refs,
            const std::vector<matchdoc::MatchDoc> &docs,
            const std::vector<Offset<flatbuffers::String>> &tracerOffsets,
            flatbuffers::FlatBufferBuilder &fbb,
            std::string &errMsg);

    static flatbuffers::Offset<FieldValueColumnTable> toFBResultRecordsByColumn(
            const matchdoc::ReferenceBase *base, const std::vector<matchdoc::MatchDoc> &docs,
            flatbuffers::FlatBufferBuilder &fbb);

    // for test
    static flatbuffers::Offset<MatchRecords> createMatchRecords(
            flatbuffers::FlatBufferBuilder &fbb,
            const std::vector<std::string>&,
            const std::vector<std::vector<int8_t>>&,
            const std::vector<std::vector<uint8_t>>&,
            const std::vector<std::vector<int16_t>>&,
            const std::vector<std::vector<uint16_t>>&,
            const std::vector<std::vector<int32_t>>&,
            const std::vector<std::vector<uint32_t>>&,
            const std::vector<std::vector<int64_t>>&,
            const std::vector<std::vector<uint64_t>>&,
            const std::vector<std::vector<float>>&,
            const std::vector<std::vector<double>>&,
            const std::vector<std::vector<std::string>>&,
            const std::vector<std::vector<std::vector<int8_t>>>&,
            const std::vector<std::vector<std::vector<uint8_t>>>&,
            const std::vector<std::vector<std::vector<int16_t>>>&,
            const std::vector<std::vector<std::vector<uint16_t>>>&,
            const std::vector<std::vector<std::vector<int32_t>>>&,
            const std::vector<std::vector<std::vector<uint32_t>>>&,
            const std::vector<std::vector<std::vector<int64_t>>>&,
            const std::vector<std::vector<std::vector<uint64_t>>>&,
            const std::vector<std::vector<std::vector<float>>>&,
            const std::vector<std::vector<std::vector<double>>>&,
            const std::vector<std::vector<std::vector<std::string>>>&,
            size_t docCount,
            const std::vector<std::string>&);

private:

    AUTIL_LOG_DECLARE();

};

}
