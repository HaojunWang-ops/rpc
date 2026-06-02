#pragma once

#include "LogStream.h"
#include "Timestamp.h"

namespace reactor
{

    class Logger
    {
    public:
        enum LogLevel
        {
            TRACE,
            DEBUG,
            INFO,
            WARN,
            ERROR,
            FATAL,
            NUM_LOG_LEVEL,
        };

        static LogLevel logLevel();
        
        class SourceFile
        {
        public:
            //两种情况，传字符串数组 和 字符串字面量
            template<int N>
            SourceFile(const char (&arr)[N])
                : data_ (arr),
                  size_ (N - 1)
            {
                const char* slash = strrchr(data_, '/');
                if (slash != NULL)
                {
                    data_ = slash + 1;
                    size_ = size_ - static_cast<int> (data_ - arr);
                }
            }
            
            SourceFile(const char* filename)
                : data_(filename)
            {
                const char* slash = strrchr(filename, '/');
                if (slash != NULL)
                {
                    data_ = slash + 1;
                }
                size_ = static_cast<int> (strlen(data_));
            }

            const char* data_;
            int size_;
        };


        Logger(SourceFile file, int line);
        Logger(SourceFile file, int line, LogLevel level);
        Logger(SourceFile file, int line, LogLevel level, const char* func);
        Logger(SourceFile file, int line, bool toAbort);
        ~Logger();

        LogStream& stream(){ return impl_.stream_; }

        typedef void (*OutputFunc)(const char* msg, int len);
        typedef void (*FlushFunc)();

        static void setLogLevel(LogLevel level);
        static void setFlush(FlushFunc);
        static void setOutput(OutputFunc);
    private:

        //真正负责将内容append到LogStream中
        class Impl
        {
        public:
            typedef Logger::LogLevel LogLevel;
            Impl(LogLevel level, int old_errno, const SourceFile& file, int line);

            void formatTime();
            void finish();

            Timestamp time_;
            LogStream stream_;
            LogLevel level_;
            int line_;
            SourceFile basename_;
        };

        Impl impl_;
    };

    extern Logger::LogLevel g_logLevel;

    inline Logger::LogLevel Logger::logLevel()
    {
        return g_logLevel;
    }

    #define LOG_TRACE if (reactor::Logger::logLevel() <= reactor::Logger::TRACE)     \
        reactor::Logger(__FILE__, __LINE__, reactor::Logger::TRACE, __func__).stream()
    #define LOG_DEBUG if (reactor::Logger::logLevel() <= reactor::Logger::DEBUG)     \
        reactor::Logger(__FILE__, __LINE__, reactor::Logger::DEBUG, __func__).stream()
    #define LOG_INFO if (reactor::Logger::logLevel() <= reactor::Logger::INFO)       \
        reactor::Logger(__FILE__, __LINE__).stream()
    #define LOG_WARN reactor::Logger(__FILE__, __LINE__, reactor::Logger::WARN).stream()
    #define LOG_ERROR reactor::Logger(__FILE__, __LINE__, reactor::Logger::ERROR).stream()
    #define LOG_FATAL reactor::Logger(__FILE__, __LINE__, reactor::Logger::FATAL).stream()
    //false true决定是否abort()
    #define LOG_SYSERR reactor::Logger(__FILE__, __LINE__, false).stream()
    #define LOG_SYSFATAL reactor::Logger(__FILE__, __LINE__, true).stream()

    const char* strerror_tl(int savedErrno);
    
    // CHECK_NOTNULL(my_ptr)
    //" ' " #val " ' Must Not Be NULL" 
    //字符串拼接 相当于" ' my_ptr ' Must Not Be NULL"
    #define CHECK_NOTNULL(val)                                                      \
        reactor::CheckNotNull(__FILE__, __LINE__, " ' " #val " ' Must Not Be NULL", val)
    template<typename T>
    T* CheckNotNull(Logger::SourceFile file, int line, const char* names, T* ptr)
    {
        if (ptr == NULL)
        {
            Logger(file, line, Logger::FATAL).stream() << names;
        }
        return ptr;
    }

}

