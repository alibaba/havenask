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
#include "indexlib/util/MMapAllocator.h"

#include <iosfwd>
#include <string>

#include "autil/EnvUtil.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MMapAllocator);

const size_t MMapAllocator::MUNMAP_SLICE_SIZE = 1024 * 1024;
bool MMapAllocator::_mmapDontDump = ("true" == autil::EnvUtil::getEnv("MMAP_DONTDUMP"));

MMapAllocator::MMapAllocator() {}

MMapAllocator::~MMapAllocator() {}
}} // namespace indexlib::util
