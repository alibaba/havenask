/**
 *   Copyright (C) The Software Authors. All rights reserved.

 *   \file     indexlib_fs_index_storage.h
 *   \author   richard.sy
 *   \date     May 2018
 *   \version  1.0.0
 *   \brief
 */

#ifndef __AITHETA_INPUT_STORAGE_H
#define __AITHETA_INPUT_STORAGE_H

#include <unordered_map>

#include <aitheta/index_params.h>
#include <aitheta/index_storage.h>
#include "indexlib/misc/log.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

class InputStorage : public aitheta::IndexStorage {
 public:
    class Handler : public aitheta::IndexStorage::Handler {
     public:
        // Destructor
        Handler(const int8_t *indexAddr, size_t size) : mIndexBaseAddr(indexAddr), mOffset(indexAddr), mSize(size) {}
        ~Handler(void) {}

     public:
        // Close the hanlder and cleanup resource
        void close(void) {}
        // Reset the hanlder (include resource), TODO(tmp solution)
        void reset(void) {}
        // Write data into the storage
        size_t write(const void *data, size_t len) { return 0; }
        // Write data into the storage with offset
        size_t write(size_t offset, const void *data, size_t len) { return 0; }
        // Read data from the storage (Zero-copy)
        size_t read(const void **data, size_t len);
        // Read data from the storage with offset (Zero-copy)
        size_t read(size_t offset, const void **data, size_t len);
        // Read data from the storage
        size_t read(void *data, size_t len);
        // Read data from the storage with offset
        size_t read(size_t offset, void *data, size_t len);
        // Retrieve size of file
        size_t size(void) const { return mSize; }

     private:
        const int8_t *const mIndexBaseAddr;
        const int8_t *mOffset;
        const size_t mSize;

     private:
        IE_LOG_DECLARE();
    };

 public:
    InputStorage(const int8_t *indexAddr) : mOffset(indexAddr) {}
    ~InputStorage(void) { cleanup(); }
    // Initialize Storage
    int init(const aitheta::StorageParams &params) override { return 0; };
    // Cleanup Storage
    int cleanup(void) override { return 0; }
    // Create a file with size
    aitheta::IndexStorage::Handler::Pointer create(const std::string &path, size_t size) override;
    // 探测路径是否存在
    bool access(const std::string &path) const override;
    // Open a file
    aitheta::IndexStorage::Handler::Pointer open(const std::string &path, bool rdonly = true) override;
    // Retrieve non-zero if the storage support random reads
    bool hasRandRead(void) const override { return false; }
    // Retrieve non-zero if the storage support random writes
    bool hasRandWrite(void) const override { return false; }

 private:
    const int8_t *mOffset;

 private:
    IE_LOG_DECLARE();
};

}
}

#endif  // __AITHETA_INDEXLIB_FS_STORAGE_H__
