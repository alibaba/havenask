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

#include "autil/legacy/jsonizable.h"

namespace suez {

// TODO: 应该把suez里的proto文件全部放到一个目录 现在依赖其它目录的proto文件如果namespace都是suez 有build问题

struct GlobalCustomInfo : autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    std::string customInfo;
};

struct HeartbeatRequest : autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    std::string globalCustomInfo;
    std::string customInfo;
    std::string signature;
};

} // namespace suez
