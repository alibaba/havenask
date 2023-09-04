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

#include <string>
#include <sys/syscall.h>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "turing_ops_util/variant/Tracer.h"

namespace suez {
namespace turing {
typedef std::map<std::string, std::string> KeyValueMap;

class DocTracer {
public:
    DocTracer(matchdoc::MatchDocAllocator *allocator);
    ~DocTracer() {}

public:
    void appendTraceToDoc(matchdoc::MatchDoc doc, const std::string &docTrace);

private:
    matchdoc::Reference<suez::turing::Tracer> *_traceRef = nullptr;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace turing

} // namespace suez