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
#include <string>
#include "fslib/common/common_define.h"
#include "fslib/Signature.h"

FSLIB_BEGIN_NAMESPACE(fs);

static const std::string fslib_version_info =
    "signature_fslib_version: " __FSLIB_VERSION;
static const std::string fslib_git_commit =
    "signature_fslib_git_commit: " __FSLIB_GIT_COMMIT;
static const std::string fslib_git_status =
    "signature_fslib_git_status: " __FSLIB_GIT_STATUS;
static const std::string fslib_build_info =
    "signature_fslib_build: " __FSLIB_BUILD_INFO;

FSLIB_END_NAMESPACE(fs);
