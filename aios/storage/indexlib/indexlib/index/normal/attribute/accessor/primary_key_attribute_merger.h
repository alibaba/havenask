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
#ifndef __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H
#define __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename Key>
class PrimaryKeyAttributeMerger : public SingleValueAttributeMerger<Key>
{
public:
    PrimaryKeyAttributeMerger(const std::string& indexName)
        : SingleValueAttributeMerger<Key>(false)
        , mIndexName(indexName)
    {
    }
    ~PrimaryKeyAttributeMerger() {}

protected:
    index::AttributeSegmentReaderWithCtx
    OpenSingleValueAttributeReader(const config::AttributeConfigPtr& attrConfig,
                                   const index_base::SegmentMergeInfo& segMergeInfo) override;

    file_system::FileWriterPtr CreateDataFileWriter(const file_system::DirectoryPtr& indexDirectory,
                                                    const std::string& temperatureLayer) override;

    std::string GetPrimaryKeyAttributeDirName(const std::string& attrName);

private:
    std::string mIndexName;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, PrimaryKeyAttributeMerger);

template <typename Key>
index::AttributeSegmentReaderWithCtx
PrimaryKeyAttributeMerger<Key>::OpenSingleValueAttributeReader(const config::AttributeConfigPtr& attrConfig,
                                                               const index_base::SegmentMergeInfo& segMergeInfo)
{
    std::shared_ptr<SingleValueAttributeSegmentReader<Key>> segReader(
        new SingleValueAttributeSegmentReader<Key>(attrConfig));
    index_base::PartitionDataPtr partData = this->mSegmentDirectory->GetPartitionData();
    index_base::SegmentData segmentData = partData->GetSegmentData(segMergeInfo.segmentId);
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(mIndexName, true);

    file_system::DirectoryPtr pkAttrDirectory =
        indexDirectory->GetDirectory(GetPrimaryKeyAttributeDirName(attrConfig->GetAttrName()), true);
    segReader->Open(segmentData, PatchApplyOption::NoPatch(), pkAttrDirectory, false);
    return {segReader, segReader->CreateReadContextPtr(nullptr)};
}

template <typename Key>
std::string PrimaryKeyAttributeMerger<Key>::GetPrimaryKeyAttributeDirName(const std::string& attrName)
{
    return std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + attrName;
}

template <typename Key>
inline file_system::FileWriterPtr
PrimaryKeyAttributeMerger<Key>::CreateDataFileWriter(const file_system::DirectoryPtr& indexDirectory,
                                                     const std::string& temperatureLayer)
{
    file_system::DirectoryPtr pkDirectory = indexDirectory->GetDirectory(mIndexName, true);
    std::string pkAttrDirName = GetPrimaryKeyAttributeDirName(this->mAttributeConfig->GetAttrName());
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    pkDirectory->RemoveDirectory(pkAttrDirName, removeOption);
    file_system::DirectoryPtr pkAttrDirectory = pkDirectory->MakeDirectory(pkAttrDirName);
    return pkAttrDirectory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARY_KEY_ATTRIBUTE_MERGER_H
