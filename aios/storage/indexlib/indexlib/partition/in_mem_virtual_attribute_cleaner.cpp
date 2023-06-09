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
#include "indexlib/partition/in_mem_virtual_attribute_cleaner.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/unused_segment_collector.h"
#include "indexlib/partition/virtual_attribute_container.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemVirtualAttributeCleaner);

InMemVirtualAttributeCleaner::InMemVirtualAttributeCleaner(const ReaderContainerPtr& readerContainer,
                                                           const VirtualAttributeContainerPtr& attrContainer,
                                                           const DirectoryPtr& directory)
    : mReaderContainer(readerContainer)
    , mVirtualAttrContainer(attrContainer)
    , mDirectory(directory)
{
}

InMemVirtualAttributeCleaner::~InMemVirtualAttributeCleaner() {}

void InMemVirtualAttributeCleaner::ClearAttributeSegmentFile(const FileList& segments,
                                                             const vector<pair<string, bool>>& attributeNames,
                                                             const DirectoryPtr& directory)
{
    for (size_t i = 0; i < segments.size(); i++) {
        string segmentPath = segments[i];
        string subSegmentPath = PathUtil::JoinPath(segments[i], SUB_SEGMENT_DIR_NAME);
        string attrDirPath = PathUtil::JoinPath(segmentPath, ATTRIBUTE_DIR_NAME);
        string subAttrDirPath = PathUtil::JoinPath(subSegmentPath, ATTRIBUTE_DIR_NAME);
        for (size_t j = 0; j < attributeNames.size(); j++) {
            string virAttrDirPath =
                PathUtil::JoinPath(attributeNames[j].second ? subAttrDirPath : attrDirPath, attributeNames[j].first);
            if (directory->IsExist(virAttrDirPath)) {
                directory->RemoveDirectory(virAttrDirPath);
            }
        }
    }
}

void InMemVirtualAttributeCleaner::Execute()
{
    FileList segments;
    UnusedSegmentCollector::Collect(mReaderContainer, mDirectory, segments);
    const vector<pair<string, bool>>& usingVirtualAttrs = mVirtualAttrContainer->GetUsingAttributes();
    const vector<pair<string, bool>>& unusingAttrs = mVirtualAttrContainer->GetUnusingAttributes();
    vector<pair<string, bool>> restAttrs, expiredAttrs;
    for (size_t i = 0; i < unusingAttrs.size(); i++) {
        if (mReaderContainer->HasAttributeReader(unusingAttrs[i].first, unusingAttrs[i].second)) {
            restAttrs.push_back(unusingAttrs[i]);
        } else {
            expiredAttrs.push_back(unusingAttrs[i]);
        }
    }

    ClearAttributeSegmentFile(segments, usingVirtualAttrs, mDirectory);
    ClearAttributeSegmentFile(segments, restAttrs, mDirectory);
    if (expiredAttrs.size() > 0) {
        FileList onDiskSegments, inMemSegments;
        DirectoryPtr inMemDirectory = mDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false);
        index_base::VersionLoader::ListSegments(mDirectory, onDiskSegments);
        index_base::VersionLoader::ListSegments(inMemDirectory, inMemSegments);
        ClearAttributeSegmentFile(onDiskSegments, expiredAttrs, mDirectory);
        ClearAttributeSegmentFile(inMemSegments, expiredAttrs, inMemDirectory);
    }
    mVirtualAttrContainer->UpdateUnusingAttributes(restAttrs);
}
}} // namespace indexlib::partition
