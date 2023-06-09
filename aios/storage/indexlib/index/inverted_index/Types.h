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
typedef int32_t format_versionid_t;
typedef uint8_t optionflag_t;
typedef uint8_t token_len_t;

enum CompressMode {
    // now we only support 4 compress mode
    PFOR_DELTA_COMPRESS_MODE = 0x00,
    REFERENCE_COMPRESS_MODE = 0x01,
    DICT_INLINE_COMPRESS_MODE = 0x02,
    SHORT_LIST_COMPRESS_MODE = 0x03,
};
enum HighFrequencyTermPostingType { hp_bitmap, hp_both };
enum PostingIteratorType {
    pi_pk,
    pi_bitmap,
    pi_buffered,
    pi_spatial,
    pi_customized,
    pi_range,
    pi_seek_and_filter,
    pi_dynamic,
    pi_composite,
    pi_unknown,
};
} // namespace indexlib::index

namespace indexlib::enum_namespace {
enum OptionFlag {
    of_none = 0,
    of_term_payload = 1,     // 1 << 0
    of_doc_payload = 2,      // 1 << 1
    of_position_payload = 4, // 1 << 2
    of_position_list = 8,    // 1 << 3
    of_term_frequency = 16,  // 1 << 4
    of_tf_bitmap = 32,       // 1 << 5
    of_fieldmap = 64,        // 1 << 6
};
} // namespace indexlib::enum_namespace

// global_enum_namespace will public to global namespace by indexlib.h, todo: rm it
namespace indexlib::global_enum_namespace {
enum PostingType {
    pt_default,
    pt_normal,
    pt_bitmap,
};
} // namespace indexlib::global_enum_namespace

namespace indexlib {
typedef uint16_t docpayload_t;
typedef uint8_t pospayload_t;
typedef uint32_t termpayload_t;
typedef uint8_t section_fid_t;
typedef uint16_t section_len_t;
typedef uint16_t section_weight_t;
typedef uint16_t sectionid_t;
typedef uint8_t optionflag_t;
typedef uint8_t fieldmap_t;
typedef uint8_t firstocc_t;
typedef uint32_t pos_t;
typedef int32_t df_t;
typedef int32_t tf_t;
typedef int64_t ttf_t;
typedef uint32_t field_len_t;
using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm
namespace indexlibv2 {
using indexlib::df_t;
using indexlib::docpayload_t;
using indexlib::fieldmap_t;
using indexlib::optionflag_t;
using indexlib::pos_t;
using indexlib::pospayload_t;
using indexlib::section_fid_t;
using indexlib::section_len_t;
using indexlib::section_weight_t;
using indexlib::sectionid_t;
using indexlib::termpayload_t;
using indexlib::tf_t;
using indexlib::ttf_t;
using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlibv2

namespace indexlibv2::index {
using indexlib::index::format_versionid_t;
}

// TODO: rm
namespace indexlibv2::config {
using indexlib::index::format_versionid_t;
using indexlib::index::optionflag_t;
using indexlib::index::token_len_t;
} // namespace indexlibv2::config
