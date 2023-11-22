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

#include "autil/Log.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/Segment.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingHolder.h"

namespace indexlibv2::index::ann {

class EmbeddingDumper
{
public:
    EmbeddingDumper() = default;
    ~EmbeddingDumper() = default;

public:
    bool Dump(const EmbeddingHolder& holder, Segment& segment);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EmbeddingDumper> EmbeddingDumperPtr;
} // namespace indexlibv2::index::ann
