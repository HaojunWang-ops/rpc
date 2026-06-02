#pragma once

#include "Mutex.h"
#include "Types.h"

#include <memory>

namespace reactor
{
    namespace FileUtil
    {
        class AppendFile;
    } //前向声明的时候，命名空间必须相同

    class LogFile : noncopyable
    {
    public:
        LogFile(const string& basename,
                off_t rollsize,
                bool threadSafe = true,
                int flushInterval = 3,
                int checkEveryN = 1024);
        ~LogFile();

        void append(const char* logfile, int len);
        void flush();
        bool rollFile();

    private:
            void append_unlock(const char* logline, int len);
            static string getLogFileName(const string& basename, time_t* now);

            const string basename_;
            const off_t rollSize_;
            const int flushInterval_;
            const int checkEveryN_;

            int count_;

            std::unique_ptr<MutexLock> mutex_;
            time_t startOfPeriod;    //当前时间所在天的零点所对应的秒数
            time_t lastRoll_;
            time_t lastFlush_;
            std::unique_ptr<FileUtil::AppendFile> file_;   

            const static int kRollPerSeconds_ = 60*60*24;
    };
}