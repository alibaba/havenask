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
#ifndef __INDEXLIB_MERGE_CONFIG_IMPL_H
#define __INDEXLIB_MERGE_CONFIG_IMPL_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/module_class_config.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class MergeConfigImpl : public autil::legacy::Jsonizable
{
public:
    MergeConfigImpl();
    MergeConfigImpl(const MergeConfigImpl& other);
    ~MergeConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator=(const MergeConfigImpl& other);
    bool IsArchiveFileEnable() const { return enableArchiveFile; }
    void SetEnableArchiveFile(bool value) { enableArchiveFile = value; }

public:
    ModuleClassConfig splitSegmentConfig;
    file_system::PackageFileTagConfigList packageFileTagConfigList; // valid when enablePackageFile == true
    uint32_t checkpointInterval; // valid when enableMergeItemCheckPoint == true && enablePackageFile == true
    bool enablePackageFile;
    bool enableInPlanMultiSegmentParallel;
    bool enableArchiveFile;
    bool needCalculateTemperature;

private:
    static const uint32_t DEFAULT_CHECKPOINT_INTERVAL = 600; // 600s

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeConfigImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_MERGE_CONFIG_IMPL_H
