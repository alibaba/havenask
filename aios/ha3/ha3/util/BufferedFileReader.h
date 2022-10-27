#ifndef ISEARCH_BUFFEREDFILEREADER_H
#define ISEARCH_BUFFEREDFILEREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

typedef enum {
    RE_NO_ERROR = 0,
    RE_EOF,
    RE_BEYOND_MAXLENTH
} ReadErrorCode;

class BufferedFileReader;
HA3_TYPEDEF_PTR(BufferedFileReader);

class BufferedFileReader
{
public:
    static const size_t BLOCK_SIZE = 4096;
    static BufferedFileReaderPtr createBufferedFileReader(const std::string &fileName);
public:
    BufferedFileReader(const std::string &fileName);
    virtual ~BufferedFileReader();
public:
    virtual ReadErrorCode readLine(char *dest, size_t maxLength, char delim, size_t &totalCount);
    virtual void seek(int64_t pos);
    virtual int64_t getPosition();
    virtual void close();
protected:
    virtual int32_t doRead(void *buffer, int32_t size) = 0;
    virtual void doSeek(int64_t pos) = 0;
    virtual int64_t doGetPosition() = 0;
private:
    void dumpSomeBuffer(char *dest, size_t &totalCount);
    const std::string& getFileName() const { return _fileName; }
private:
    std::string _fileName;
    char *_buffer;
    size_t _bufferSize;
    size_t _startPos;
    size_t _endPos;
    size_t _currentPos;
    bool _isEof;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(util);

#endif //ISEARCH_BUFFEREDFILEREADER_H
