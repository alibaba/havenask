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
#include "indexlib/index_base/index_meta/segment_info.h"

#include "indexlib/document/document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Timer.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::file_system;
using namespace indexlib::document;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentInfo);

SegmentInfo& SegmentInfo::operator=(const indexlibv2::framework::SegmentInfo& other)
{
    indexlibv2::framework::SegmentInfo::operator=(other);
    return *this;
}

std::string SegmentInfo::ToString(bool compactFormat) const { return ToJsonString(*this, compactFormat); }
void SegmentInfo::FromString(const std::string& str) { FromJsonString(*this, str); }

bool SegmentInfo::Load(const indexlib::file_system::DirectoryPtr& directory)
{
    string content;
    try {
        directory->Load(SEGMENT_INFO_FILE_NAME, content, true);
    } catch (const indexlib::util::NonExistException& e) {
        IE_LOG(ERROR, "segment info not exist in dir [%s]", directory->DebugString().c_str());
        return false;
    } catch (...) {
        throw;
    }

    FromString(content);
    return true;
}

void SegmentInfo::Store(const indexlib::file_system::DirectoryPtr& directory, bool removeIfExist) const
{
    if (removeIfExist) {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        directory->RemoveFile(SEGMENT_INFO_FILE_NAME, removeOption);
    }
    string content = ToString();
    indexlib::file_system::WriterOption writerOption = indexlib::file_system::WriterOption::AtomicDump();
    writerOption.notInPackage = true;
    directory->Store(SEGMENT_INFO_FILE_NAME, content, writerOption);
}

// TODO: used in BitmapPostingMaker::InnerMakeOneSegmentData
void SegmentInfo::TEST_Store(const string& path) const
{
    string content = ToString();
    FslibWrapper::AtomicStoreE(path, content);
}
void SegmentInfo::TEST_Load(std::string path)
{
    if (!autil::StringUtil::endsWith(path, SEGMENT_INFO_FILE_NAME)) {
        path += std::string(SEGMENT_INFO_FILE_NAME);
    }
    string content;
    auto ec = FslibWrapper::AtomicLoad(path, content).Code();
    THROW_IF_FS_ERROR(ec, "load SegmentInfo from [%s] failed", path.c_str());
    FromString(content);
}

void SegmentInfo::Update(const DocumentPtr& doc)
{
    const auto& docLocator = doc->GetLocatorV2();
    if (docLocator.IsValid()) {
        SetLocator(docLocator);
    }
    if (timestamp < doc->GetTimestamp()) {
        timestamp = doc->GetTimestamp();
    }
    if (maxTTL < doc->GetTTL()) {
        maxTTL = doc->GetTTL();
    }
}

bool SegmentInfo::GetCompressFingerPrint(uint64_t& fingerPrint) const
{
    std::string value;
    if (GetDescription(SEGMENT_COMPRESS_FINGER_PRINT, value)) {
        if (!autil::StringUtil::fromString(value, fingerPrint)) {
            AUTIL_LOG(ERROR, "invalid compress_finger_print [%s]", value.c_str());
            return false;
        }
        return true;
    }
    return false;
}

bool SegmentInfo::GetOriginalTemperatureMeta(SegmentTemperatureMeta& meta) const
{
    string value;
    if (GetDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP, value)) {
        json::JsonMap temperatureMetrics;
        try {
            FromJsonString(temperatureMetrics, value);
        } catch (...) {
            IE_LOG(ERROR, "jsonize metric [%s] failed", value.c_str());
            return false;
        }
        return SegmentTemperatureMeta::InitFromJsonMap(temperatureMetrics, meta);
    }
    return false;
}

}} // namespace indexlib::index_base
