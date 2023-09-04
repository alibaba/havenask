#include "swift/broker/storage_dfs/test/FakeMessageCommitter.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <sys/types.h>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "swift/broker/storage_dfs/MessageCommitter.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/MemoryMessage.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace config {
class PartitionConfig;
} // namespace config
namespace monitor {
class BrokerMetricsReporter;
} // namespace monitor
namespace storage {
class FileManager;
} // namespace storage
} // namespace swift

using namespace std;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FakeMessageCommitter);

FakeMessageCommitter::FakeMessageCommitter(const protocol::PartitionId &partitionId,
                                           const config::PartitionConfig &config,
                                           FileManager *fileManager,
                                           monitor::BrokerMetricsReporter *amonMetricsReporter)
    : MessageCommitter(partitionId, config, fileManager, amonMetricsReporter) {}

FakeMessageCommitter::~FakeMessageCommitter() {}

ErrorCode FakeMessageCommitter::appendMsgForDataFile(const MemoryMessage &msg, int64_t writeLen) {
    if (NULL == _dataOutputFile) {
        return ERROR_BROKER_CLOSE_FILE;
    }

    int64_t leftLen = (int64_t)msg.getLen() > writeLen ? writeLen : (int64_t)msg.getLen();
    BlockPtr block = msg.getBlock();
    int64_t offset = msg.getOffset();
    while (leftLen > 0) {
        int64_t count = std::min(leftLen, block->getSize() - offset);
        ssize_t writeLen = _dataOutputFile->write(block->getBuffer() + offset, count);
        if (writeLen == -1) {
            AUTIL_LOG(WARN, "append data file fail");
            return ERROR_BROKER_WRITE_FILE;
        }
        offset += writeLen;
        leftLen -= writeLen;
        if (offset == block->getSize()) {
            block = block->getNext();
            offset = 0;
        }
    }

    fslib::ErrorCode ec = _dataOutputFile->flush();
    if (ec != fslib::EC_OK) {
        return ERROR_BROKER_COMMIT_FILE;
    }

    return ERROR_NONE;
}

ErrorCode FakeMessageCommitter::appendMsgMetaForMetaFile(const MemoryMessage &msg, int64_t writeLen) {
    if (NULL == _metaOutputFile) {
        return ERROR_BROKER_CLOSE_FILE;
    }

    FileMessageMeta fmm;
    fmm.timestamp = msg.getTimestamp();
    fmm.endPos = _writedSize + msg.getLen();
    fmm.payload = msg.getPayload();
    fmm.maskPayload = msg.getMaskPayload();
    fmm.isCompress = msg.isCompress();
    _writedSize += msg.getLen();
    _writedSizeForReport += msg.getLen();
    _writedId = msg.getMsgId();

    char buf[FILE_MESSAGE_META_SIZE];
    fmm.toBuf(buf);

    int64_t totalLen = FILE_MESSAGE_META_SIZE > writeLen ? writeLen : FILE_MESSAGE_META_SIZE;
    int64_t leftLen = totalLen;
    while (leftLen > 0) {
        ssize_t writeLen = _metaOutputFile->write(&buf[0] + totalLen - leftLen, leftLen);
        if (writeLen == -1) {
            AUTIL_LOG(WARN, "append meta file fail");
            return ERROR_BROKER_WRITE_FILE;
        }
        leftLen -= writeLen;
    }
    fslib::ErrorCode ec = _metaOutputFile->flush();
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(WARN, "commit file [%s] failed", _metaOutputFile->getFileName());
        return ERROR_BROKER_COMMIT_FILE;
    }

    return ERROR_NONE;
}

} // namespace storage
} // namespace swift
