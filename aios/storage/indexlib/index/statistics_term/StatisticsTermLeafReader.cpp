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
#include "indexlib/index/statistics_term/StatisticsTermLeafReader.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermFormatter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, StatisticsTermLeafReader);

Status StatisticsTermLeafReader::Open(const std::shared_ptr<StatisticsTermIndexConfig>& indexConfig,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& indexRootDir)
{
    const auto& indexName = indexConfig->GetIndexName();
    auto [status, indexDir] = indexRootDir->GetDirectory(indexName).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "get index dir %s failed.", indexName.c_str());
    if (!indexDir) {
        return Status::Corruption("index dir %s does not exist in %s", indexName.c_str(),
                                  indexRootDir->DebugString().c_str());
    }

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    auto [s, fileReader] = indexDir->CreateFileReader(DATA_FILE, readerOption).StatusWith();
    RETURN_IF_STATUS_ERROR(s, "open file reader failed.");

    _dataFile = fileReader;

    RETURN_IF_STATUS_ERROR(LoadMeta(indexDir), "load term meta failed.");

    return Status::OK();
}

Status StatisticsTermLeafReader::LoadMeta(const std::shared_ptr<indexlib::file_system::IDirectory>& indexDir)
{
    std::string content;
    auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
    readerOption.mayNonExist = true;
    auto fsResult = indexDir->Load(META_FILE, readerOption, content);
    if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_OK &&
        fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_NOENT) {
        AUTIL_LOG(ERROR, "load statistics term meta failed, dir:%s", indexDir->DebugString().c_str());
        return fsResult.Status();
    }

    if (content.empty()) {
        // not exist
        return Status::OK();
    }

    try {
        autil::legacy::FromJsonString(_termMetas, content);
    } catch (...) {
        AUTIL_LOG(ERROR, "load term meta failed, content[%s]", content.c_str());
        return Status::InternalError("deserialize term meta failed.");
    }
    return Status::OK();
}

Status StatisticsTermLeafReader::GetTermStatistics(const std::string& invertedIndexName,
                                                   std::unordered_map<size_t, std::string>& termStatistics)
{
    auto it = _termMetas.find(invertedIndexName);
    if (it == _termMetas.end()) {
        return Status::OK();
    }

    auto [offset, blockLen] = it->second;
    auto buf = std::make_unique<char[]>(blockLen);
    auto [st, actualLen] = _dataFile->Read(buf.get(), blockLen, offset).StatusWith();
    RETURN_IF_STATUS_ERROR(st, "read block failed, offset[%lu] len[%lu]", offset, blockLen);
    if (actualLen != blockLen) {
        AUTIL_LOG(ERROR, "mismatch len, expect[%lu] actual[%lu]", blockLen, actualLen);
        return Status::InternalError("mismatch len");
    }

    const std::string str(buf.get(), blockLen);
    auto pos = str.find('\n');
    if (pos == std::string::npos) {
        AUTIL_LOG(ERROR, "invalid format, offset[%lu], len[%lu] data[%s]", offset, blockLen, str.c_str());
        return Status::InternalError("invalid format");
    }
    const std::string headerLine = str.substr(0, pos);
    pos += 1;

    std::string actualIndexName;
    size_t itemCount = 0;
    if (!StatisticsTermFormatter::ParseHeaderLine(headerLine, actualIndexName, itemCount).IsOK()) {
        AUTIL_LOG(ERROR, "invalid format, offset[%lu], len[%lu] line[%s]", offset, blockLen, headerLine.c_str());
        return Status::InternalError("invalid format");
    }
    if (invertedIndexName != actualIndexName) {
        AUTIL_LOG(ERROR, "mismatch inverted name, expect[%s] actual[%s]", invertedIndexName.c_str(),
                  actualIndexName.c_str());
        return Status::InternalError("mismatch inverted index name");
    }

    for (size_t i = 0; i < itemCount; ++i) {
        size_t termHash = 0;
        std::string term;
        if (!StatisticsTermFormatter::ParseDataLine(str, pos, term, termHash).IsOK()) {
            AUTIL_LOG(ERROR, "parse data line failed, str[%s], pos[%lu]", str.c_str(), pos);
            return Status::InternalError("parse data line failed");
        }
        termStatistics[termHash] = term;
    }
    return Status::OK();
}

} // namespace indexlibv2::index
