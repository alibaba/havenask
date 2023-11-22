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
#include "indexlib/merger/segment_directory_finder.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SegmentDirectoryFinder);
using FsDirectoryPtr = file_system::DirectoryPtr;
SegmentDirectoryFinder::SegmentDirectoryFinder() {}

SegmentDirectoryFinder::~SegmentDirectoryFinder() {}

file_system::DirectoryPtr SegmentDirectoryFinder::GetAttributeDir(const SegmentDirectoryPtr& segDir,
                                                                  const AttributeConfig* attrConfig, segmentid_t segId)
{
    auto dir = segDir->GetSegmentDirectory(segId);
    if (!dir) {
        // TODO: throw exception
        return dir;
    }

    const string& attrName = attrConfig->GetAttrName();
    AttributeConfig::ConfigType configType = attrConfig->GetConfigType();
    if (configType == AttributeConfig::ct_section) {
        std::string attributePath = index::INDEX_DIR_NAME + '/' + attrName + '/';
        return dir->GetDirectory(attributePath, false);
    }

    // TODO: add pk type
    assert(configType == AttributeConfig::ct_normal);
    std::string attributePath = index::ATTRIBUTE_DIR_NAME + '/' + attrName + '/';
    return dir->GetDirectory(attributePath, false);
}

segmentid_t SegmentDirectoryFinder::GetSegmentIdFromDelMapDataFile(const SegmentDirectoryPtr& segDir,
                                                                   segmentid_t segmentId,
                                                                   const std::string& dataFileName)
{
    size_t pos = dataFileName.find('_');
    if (pos == string::npos) {
        IE_LOG(WARN, "Error file name in deletion map: [%s].", dataFileName.c_str());
        return INVALID_SEGMENTID;
    }
    segmentid_t physicalSegId;
    bool ret = StringUtil::strToInt32(dataFileName.substr(pos + 1).c_str(), physicalSegId);
    if (ret) {
        return segDir->GetVirtualSegmentId(segmentId, physicalSegId);
    }
    return INVALID_SEGMENTID;
}

FsDirectoryPtr SegmentDirectoryFinder::MakeSegmentDirectory(const FsDirectoryPtr& rootDirectory, segmentid_t segId,
                                                            const Version& version, bool isSub)
{
    string segDirName = version.GetSegmentDirName(segId);
    auto dir = rootDirectory->GetDirectory(segDirName, true);
    if (dir == nullptr) {
        return file_system::DirectoryPtr();
    }

    if (isSub) {
        dir = dir->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
    }
    return dir;
}
}} // namespace indexlib::merger
