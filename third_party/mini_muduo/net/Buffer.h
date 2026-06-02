#pragma once

#include "base/copyable.h"

#include <algorithm>
#include <string>
#include <vector>

#include <assert.h>

namespace reactor
{
    namespace net
    {
        class Buffer : copyable
        {
        public:
            static const size_t kCheapPrepend = 8;
            static const size_t kInitialSize = 1024;

            Buffer()
                : buffer_ (kCheapPrepend + kInitialSize),
                  readIndex_(kCheapPrepend),
                  writeIndex_(kCheapPrepend)
            {
                assert(readableBytes() == 0);
                assert(writeableBytes() == kInitialSize);
                assert(prependableBytes() == kCheapPrepend);
            }

            void swap(Buffer& rhs)
            {
                buffer_.swap(rhs.buffer_);
                std::swap(readIndex_, rhs.readIndex_);
                std::swap(writeIndex_, rhs.writeIndex_);
            }

            size_t readableBytes() const
            {
                return writeIndex_ - readIndex_;
            }
            size_t writeableBytes() const
            {
                return buffer_.size() - writeIndex_;
            }
            size_t prependableBytes() const
            {
                return readIndex_;
            }

            const char* peek() const
            {
                return begin() + readIndex_;
            }

            void retrieve(size_t len)
            {
                assert(len <= readableBytes());
                readIndex_ += len;
            }

            void retrieveUntil(const char* end)
            {
                assert(peek() <= end);
                assert(end <= beginWrite());
                retrieve(end - peek());
            }

            void retrieveAll()
            {
                readIndex_ = kCheapPrepend;
                writeIndex_ = kCheapPrepend;
            }

            std::string retrieveAsString()
            {
                std::string str(peek(), readableBytes());
                retrieveAll();
                return str;
            }

            void append(const std::string& str)
            {
                append(str.data(), str.length());
            }

            void append(const char* data, size_t len)
            {
                ensureWriteableBytes(len);
                std::copy(data, data + len, beginWrite());
                hasWritten(len);
            }

            void append(const void* data, size_t len)
            {
                append(static_cast<const char*>(data), len);
            }

            void ensureWriteableBytes(size_t len)
            {
                if (writeableBytes() < len)
                {
                    makeSpace(len);
                }
                assert(writeableBytes() >= len);
            }

            char* beginWrite()
            {
                return begin() + writeIndex_;
            }
            const char* beginwrite() const
            {
                return begin() + writeIndex_;
            }
            void hasWritten(size_t len)
            {
                writeIndex_ += len;
            }

            //在prependalbe部分添加头部
            void prepend(const void* data, size_t len)
            {
                assert(len <= prependableBytes());
                readIndex_ -= len;
                const char* d = static_cast<const char*> (data);
                std::copy(d, d + len, begin() + readIndex_);
            }

            //将扩大后的vector缩小
            void shrink(size_t reserve)
            {
                std::vector<char> buf(kCheapPrepend + readableBytes() + reserve);
                std::copy(peek(), peek() + readableBytes(), buf.begin() + kCheapPrepend);
                buf.swap(buffer_);
            }

            ssize_t readFd(int fd, int* savedErrno);
        private:
            char* begin() { return buffer_.data(); }
            const char* begin() const { return buffer_.data(); }

            void makeSpace(size_t len)
            {
                if (writeableBytes() + prependableBytes() < len + kCheapPrepend)
                {
                    buffer_.resize(writeIndex_ + len);
                }
                else
                {
                    assert(kCheapPrepend < readIndex_);
                    size_t readable = readableBytes();
                    std::copy(begin() + readIndex_, begin() + writeIndex_, begin() + kCheapPrepend);
                    readIndex_ = kCheapPrepend;
                    writeIndex_ = readIndex_ + readable;
                    assert(readableBytes() == readable);
                }
            }
        private:
            std::vector<char> buffer_;
            size_t readIndex_;
            size_t writeIndex_; 
        };
    }
}