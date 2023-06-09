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

namespace indexlib::index {
enum PrimaryKeyHashType {
    pk_murmur_hash,
    pk_number_hash,
    pk_default_hash,
};
}

namespace indexlibv2::index {
// TODO: rm
using indexlib::index::pk_default_hash;
using indexlib::index::pk_murmur_hash;
using indexlib::index::pk_number_hash;
using indexlib::index::PrimaryKeyHashType;
} // namespace indexlibv2::index
