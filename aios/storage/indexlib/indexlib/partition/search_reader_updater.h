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

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/table_reader_container_updater.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);

namespace indexlib { namespace partition {

class SearchReaderUpdater
{
public:
    SearchReaderUpdater(std::string schemaName);
    ~SearchReaderUpdater();

public:
    bool AddReaderUpdater(TableReaderContainerUpdater* updater);
    void RemoveReaderUpdater(TableReaderContainerUpdater* updater);
    bool Update(const IndexPartitionReaderPtr& reader);
    void FillVirtualAttributeConfigs(std::vector<config::AttributeConfigPtr>& mainAttributeConfigs,
                                     std::vector<config::AttributeConfigPtr>& subAttributeConfigs);
    bool NeedUpdateReader() { return mNeedUpdateReader; }
    size_t Size() { return mReaderUpdaters.size(); }
    bool HasUpdater(TableReaderContainerUpdater* updater)
    {
        for (size_t i = 0; i < mReaderUpdaters.size(); i++) {
            if (updater == mReaderUpdaters[i]) {
                return true;
            }
        }
        return false;
    }

private:
    void ConvertAttributesToNames(std::vector<config::AttributeConfigPtr>& mainAttributeConfigs,
                                  std::set<std::string>& mainAttrNames);
    void MergeAttrConfigs(const std::vector<config::AttributeConfigPtr>& srcAttrConfigs,
                          std::set<std::string>& attrNames, std::vector<config::AttributeConfigPtr>& targetAttrConfigs);

private:
    std::vector<TableReaderContainerUpdater*> mReaderUpdaters;
    mutable autil::ThreadMutex mLock;
    std::string mSchemaName;
    bool mHasAuxTable;
    bool mIsAuxTable;
    bool mNeedUpdateReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchReaderUpdater);
}} // namespace indexlib::partition
