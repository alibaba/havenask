#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);

IE_LOG_SETUP(partition, AttributePatchDataWriter);

FileWriter* AttributePatchDataWriter::CreateBufferedFileWriter(
        size_t bufferSize, bool asyncWrite) {
    BufferedFileWriter* fileWriter = new file_system::BufferedFileWriter;
    fileWriter->ResetBufferParam(bufferSize, asyncWrite);
    return fileWriter;
}

IE_NAMESPACE_END(partition);
