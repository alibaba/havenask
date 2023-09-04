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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace fslib {

namespace fs {
class File;
} // namespace fs
} // namespace fslib

namespace swift {
namespace util {

constexpr uint8_t UNIT = 7u;
constexpr uint8_t UNIT_COUNT = (64 + UNIT - 1) / UNIT;
constexpr char ARGS_SEP = '\0';

class InlineVersion {
public:
    InlineVersion();
    InlineVersion(uint64_t masterVersion, uint64_t partVersion);

public:
    bool deserialize(const std::string &inlineVersion);
    std::string serialize() const;
    void fromProto(const protocol::InlineVersion &inlineVersion);
    protocol::InlineVersion toProto();

public:
    bool valid() const;
    uint64_t getMasterVersion() const;
    uint64_t getPartVersion() const;
    void setMasterVersion(uint64_t value);

public:
    std::string toDebugString() const;

public:
    bool operator<(const InlineVersion &lh) const;
    bool operator==(const InlineVersion &lh) const;

private:
    uint64_t _masterVersion = 0;
    uint64_t _partVersion = 0;
};

class PanguInlineFileUtil {
public:
    PanguInlineFileUtil();
    ~PanguInlineFileUtil();
    PanguInlineFileUtil(const PanguInlineFileUtil &) = delete;
    PanguInlineFileUtil &operator=(const PanguInlineFileUtil &) = delete;

public:
    static fslib::ErrorCode isInlineFileExist(const std::string &filePath, bool &isExist);
    static fslib::ErrorCode getInlineFile(const std::string &filePath, std::string &output);
    static fslib::ErrorCode updateInlineFile(const std::string &filePath, uint64_t oldVersion, uint64_t newVersion);
    static fslib::ErrorCode updateInlineFile(const std::string &filePath,
                                             const InlineVersion &oldInlineVersion,
                                             const InlineVersion &newInlineVersion);
    static fslib::fs::File *openFileForWrite(const std::string &fileName,
                                             const std::string &inlineFile,
                                             const std::string &inlineVersion,
                                             ::fslib::ErrorCode &ec);
    static fslib::ErrorCode sealFile(const std::string &filePath);
    static void setInlineFileMock(bool needMock); // for local test case

private:
    static bool isInlineFileMock(); // for local test case
    static fslib::ErrorCode
    updateInlineFile(const std::string &filePath, const std::string &oldFile, const std::string &newFile);
    static fslib::ErrorCode
    panguStatInlinefile(const std::string &filePath, const std::string &args, std::string &output);
    static fslib::ErrorCode
    panguCreateInlinefile(const std::string &filePath, const std::string &args, std::string &output);
    static fslib::ErrorCode
    panguUpdateInlinefileCAS(const std::string &filePath, const std::string &args, std::string &output);
    static fslib::ErrorCode
    panguOpenForWriteWithInlinefile(const std::string &filePath, const std::string &args, std::string &output);

private:
    static fslib::ErrorCode loadFile(const std::string &filePath, std::string &fileContent) noexcept;
    static fslib::ErrorCode storeFile(const std::string &filePath, const std::string &data) noexcept;

private:
    static bool _mockInlineFile; // for local test case

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(PanguInlineFileUtil);

} // end namespace util
} // end namespace swift
