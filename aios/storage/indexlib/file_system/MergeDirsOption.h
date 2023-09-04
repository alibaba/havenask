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

#include <stdint.h>

namespace indexlib { namespace file_system {
struct FenceContext;

struct MergeDirsOption {
    bool mergePackageFiles = false;
    FenceContext* fenceContext = nullptr; // FenceContext::NoFence

    static MergeDirsOption MergePackage() { return {true, nullptr}; }
    static MergeDirsOption NoMergePackage() { return {false, nullptr}; }
    static MergeDirsOption MergePackageWithFence(FenceContext* fenceContext) { return {true, fenceContext}; }
    static MergeDirsOption NoMergePackageWithFence(FenceContext* fenceContext) { return {false, fenceContext}; }
};

}} // namespace indexlib::file_system
