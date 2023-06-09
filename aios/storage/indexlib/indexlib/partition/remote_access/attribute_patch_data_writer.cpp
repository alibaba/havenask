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
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"

#include "indexlib/file_system/file/BufferedFileWriter.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;

namespace indexlib { namespace partition {

IE_LOG_SETUP(partition, AttributePatchDataWriter);

FileWriter* AttributePatchDataWriter::CreateBufferedFileWriter(size_t bufferSize, bool asyncWrite)
{
    BufferedFileWriter* fileWriter = new file_system::BufferedFileWriter;
    fileWriter->ResetBufferParam(bufferSize, asyncWrite).GetOrThrow();
    return fileWriter;
}
}} // namespace indexlib::partition
