#include <atomic>
#include <cstdlib>
#include <map>
#include <memory>

#include "autil/LambdaWorkItem.h"
#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "dadi-cache/dadi.h"
#include "string.h"

using namespace std;

struct DadiFs {
    map<pair<string, off_t>, string> data; // fileName,offset -> fileData
    size_t pageSize = 16384;
    autil::ReadWriteLock lock;
    autil::ThreadPool threadPool;

    DadiFs() : threadPool(1, autil::ThreadPool::DEFAULT_QUEUESIZE) {}
};

struct DadiFile {
    DadiFs* fs;
    string fileName;
};

/************************** Exception/Eviction Switch **********************/
static atomic<bool> writeFailSwitch;
static atomic<bool> readFailSwitch;
static atomic<bool> evictSwitch;

void SetWriteFail(bool enableFail) { writeFailSwitch = enableFail; }
void SetReadFail(bool enableFail) { readFailSwitch = enableFail; }
void SetEvict(bool enableEvict) { evictSwitch = enableEvict; }

/************************** Interface  **********************/
void DestroyDadiEnv() {}

DadiFsHandle CreateDadiFs(const char* params)
{
    // only use page size
    string paramStr(params);
    size_t pageSize = 16384; // 16KB

    const string PAGE_SIZE = "page_size";
    size_t pos = paramStr.find(PAGE_SIZE);
    if (pos != string::npos) {
        pos += PAGE_SIZE.size() + 1; // skip =
        pageSize = 0;
        while (pos < paramStr.size() && paramStr[pos] != ',') {
            pageSize = pageSize * 10 + paramStr[pos] - '0';
            pos++;
        }
    }
    auto fs = new DadiFs();
    fs->threadPool.start("fake_dadi");
    fs->pageSize = pageSize;
    return reinterpret_cast<DadiFsHandle>(fs);
}

void DestroyDadiFs(DadiFsHandle fs)
{
    auto dadiFs = reinterpret_cast<DadiFs*>(fs);
    dadiFs->threadPool.stop();
    delete dadiFs;
}

DadiFileHandle DadiOpenFile(DadiFsHandle fs, const char* filename, int flags)
{
    auto file = new DadiFile();
    file->fs = reinterpret_cast<DadiFs*>(fs);
    file->fileName = filename;
    return reinterpret_cast<DadiFileHandle>(file);
}

ssize_t DadiFileRead(DadiFileHandle file, void* buf, size_t count, off_t offset, int flags)
{
    if (readFailSwitch) {
        return 0;
    }

    auto dadiFile = reinterpret_cast<DadiFile*>(file);
    DadiFs* fs = dadiFile->fs;

    // in indexlib, we have this limit : count == page size, offset = 0 (mod page size)
    if (count != fs->pageSize) {
        return 0;
    }

    if (offset % fs->pageSize) {
        return 0;
    }

    autil::ScopedReadLock lock(fs->lock);
    auto iter = fs->data.find({dadiFile->fileName, offset});
    if (iter == fs->data.end()) {
        return 0;
    }
    string ret = iter->second;
    size_t readLength = min(count, ret.length());
    memcpy(buf, ret.data(), readLength);
    return readLength;
}

ssize_t DadiFilePWrite(DadiFileHandle file, const void* buf, size_t count, off_t offset, int flags)
{
    if (writeFailSwitch) {
        return 0;
    }

    auto dadiFile = reinterpret_cast<DadiFile*>(file);
    DadiFs* fs = dadiFile->fs;

    // in indexlib, we have this limit : count == page size, offset = 0 (mod page size)
    if (count != fs->pageSize) {
        return 0;
    }

    if (offset % fs->pageSize) {
        return 0;
    }

    string writeValue((char*)buf, count);
    autil::ScopedWriteLock lock(fs->lock);
    string value = fs->data[{dadiFile->fileName, offset}];
    if (value == "") {
        fs->data[{dadiFile->fileName, offset}] = writeValue;
        if (evictSwitch && (rand() & 1)) {
            // if evict switch turn on, 50% we will evict the smallest key
            fs->data.erase(fs->data.begin());
        }
        return count;
    }
    // in indexlib, we add a limit, no update old block
    return value == writeValue ? count : 0;
}

void DadiFileAsyncRead(DadiFileHandle file, void* buf, size_t count, off_t offset, int flags,
                       dadi_rw_callback_t callback, void* userdata)
{
    auto dadiFile = reinterpret_cast<DadiFile*>(file);
    DadiFs* fs = dadiFile->fs;
    auto ec =
        fs->threadPool.pushWorkItem(new autil::LambdaWorkItem([file, buf, count, offset, flags, callback, userdata]() {
            usleep(100);
            ssize_t nread = DadiFileRead(file, buf, count, offset, flags);
            callback(userdata, nread);
        }));
    (void)ec;
}

void DadiFileAsyncPWrite(DadiFileHandle file, const void* buf, size_t count, off_t offset, int flags,
                         dadi_rw_callback_t callback, void* userdata)
{
    auto dadiFile = reinterpret_cast<DadiFile*>(file);
    DadiFs* fs = dadiFile->fs;
    auto ec =
        fs->threadPool.pushWorkItem(new autil::LambdaWorkItem([file, buf, count, offset, flags, callback, userdata]() {
            usleep(100);
            ssize_t nwrite = DadiFilePWrite(file, buf, count, offset, flags);
            callback(userdata, nwrite);
        }));
    (void)ec;
}

int DadiCloseFile(DadiFileHandle file)
{
    auto dadiFile = reinterpret_cast<DadiFile*>(file);
    // insure previous read task finished
    dadiFile->fs->threadPool.pushWorkItem(new autil::LambdaWorkItem([dadiFile]() { delete dadiFile; }));
    return 0;
}
