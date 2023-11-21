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
#include "autil/Diagnostic.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wsuggest-override")
#include <aitheta2/index_ckpt_manager.h>
DIAGNOSTIC_POP

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"

namespace aitheta2 {
class CustomizedCkptManager : public IndexCkptManager
{
public:
    //! Constructor
    CustomizedCkptManager(const std::shared_ptr<indexlib::file_system::Directory>& rootDirectory)
        : _rootDirectory(rootDirectory)
    {
    } //! Destructor
    ~CustomizedCkptManager(void) = default;

    //! Initialize dumper
    int init(const IndexParams& params, const std::string& ckptDirName) override;

    //! Create ckpt dumper to save index builder's ckpt
    IndexDumper::Pointer create_ckpt_dumper(const std::string& ckptId) override;

    //! Mark done when ckpt dumper closed
    int dump_ckpt_finish(const std::string& ckptId) override;

    //! Get ckpt container to recover index builder
    IndexContainer::Pointer get_ckpt_container(const std::string& ckptId) override;

    //! Get ckpt ids
    std::vector<std::string> get_ckpt_ids() override;

    //! Cleanup all ckpts
    int cleanup(void) override;

private:
    bool MakeDoneFlagFile(const std::string& ckptId);
    bool MakeCkptDirectory();

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDirectory;
    std::shared_ptr<indexlib::file_system::Directory> _ckptDirectory;
    std::string _ckptDirName;
    IndexParams _params;
    bool _disableCleanup {false};

    std::map<std::string, std::shared_ptr<indexlib::file_system::FileWriter>> _ckptFileWriters;
    inline static const std::string DONE_FILE_SUFFIX = ".done";
    inline static const std::string DONE_FILE_PATTERN = ".+\\.done$";
    inline static const std::string DISABLE_CLEANUP = "disable_cleanup";
};

} // namespace aitheta2
