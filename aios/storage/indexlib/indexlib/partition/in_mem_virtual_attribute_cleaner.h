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

#include "fslib/common/common_type.h"
#include "indexlib/common/executor.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, VirtualAttributeContainer);
DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class InMemVirtualAttributeCleaner : public common::Executor
{
public:
    InMemVirtualAttributeCleaner(const ReaderContainerPtr& readerContainer,
                                 const VirtualAttributeContainerPtr& attrContainer,
                                 const file_system::DirectoryPtr& directory);

    ~InMemVirtualAttributeCleaner();

public:
    void Execute() override;

private:
    void ClearAttributeSegmentFile(const fslib::FileList& segments,
                                   const std::vector<std::pair<std::string, bool>>& attributeNames,
                                   const file_system::DirectoryPtr& directory);

private:
    ReaderContainerPtr mReaderContainer;
    VirtualAttributeContainerPtr mVirtualAttrContainer;
    file_system::DirectoryPtr mDirectory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemVirtualAttributeCleaner);
}} // namespace indexlib::partition
