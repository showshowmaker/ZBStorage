#define _GNU_SOURCE
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <set>
#include <queue>
#include <list>
#include <array>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <vector>
#include <map>
#include <sys/types.h>
#include <sys/file.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fstream>
#include <algorithm>
#include <thread>
#include <future>
#include <unistd.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include "masstree_wrapper.h" 
#include <filesystem>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <utility>
#include <atomic>
#define BIG_CONSTANT(x) (x##LLU)
using namespace std;
//namespace fs = filesystem;
#define pagesize 64
#define keysize 16
#define valuesize 234
#define userfilenum 1000000
constexpr int leafnum = (pagesize * 1024 - sizeof(uint8_t)) / (keysize + valuesize);
using sizetype = std::conditional_t<(leafnum >= 256), uint16_t, uint8_t>;
#define sizeofsize sizeof(sizetype)//16
#define leafnum ((pagesize*1024-sizeofsize)/(keysize+valuesize))//262
#define leafcount ((userfilenum+leafnum-1)/leafnum)//3817
#define emptynum (pagesize*1024-leafnum*(keysize+valuesize)-sizeofsize)

template <typename T>
class SafeQueue
{
private:
    std::queue<T> m_queue; //利用模板函数构造队列
    std::mutex m_mutex; // 访问互斥信号量
public:
    SafeQueue() {}
    SafeQueue(SafeQueue &&other) {}
    ~SafeQueue() {}
    bool empty() // 返回队列是否为空
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 互斥信号变量加锁，防止m_queue被改变
        return m_queue.empty();
    }
    int size()
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 互斥信号变量加锁，防止m_queue被改变
        return m_queue.size();
    }
    // 队列添加元素
    void enqueue(T &t)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.emplace(t);
    }
    // 队列取出元素
    bool dequeue(T &t)
    {
        std::unique_lock<std::mutex> lock(m_mutex); // 队列加锁
        if (m_queue.empty())
            return false;
        t = std::move(m_queue.front()); // 取出队首元素，返回队首元素值，并进行右值引用
        m_queue.pop(); // 弹出入队的第一个元素
        return true;
    }
};
class ThreadPool
{
private:
    class ThreadWorker // 内置线程工作类
    {
    private:
        int m_id; // 工作id
        ThreadPool *m_pool; // 所属线程池
    public:
        // 构造函数
        ThreadWorker(ThreadPool *pool, const int id) : m_pool(pool), m_id(id)
        {
        }
        // 重载()操作
        void operator()()
        {
            std::function<void()> func; // 定义基础函数类func
            bool dequeued; // 是否正在取出队列中元素
            while (!m_pool->m_shutdown)
            {
                {
                    // 为线程环境加锁，互访问工作线程的休眠和唤醒
                    std::unique_lock<std::mutex> lock(m_pool->m_conditional_mutex);
                    // 如果任务队列为空，阻塞当前线程
                    if (m_pool->m_queue.empty())
                    {
                        m_pool->m_conditional_lock.wait(lock); // 等待条件变量通知，开启线程
                    }
                    // 取出任务队列中的元素
                    dequeued = m_pool->m_queue.dequeue(func);
                }
                // 如果成功取出，执行工作函数
                if (dequeued)
                    func();
            }
        }
    };
    bool m_shutdown; // 线程池是否关闭
    SafeQueue<std::function<void()>> m_queue; // 执行函数安全队列，即任务队列
    std::vector<std::thread> m_threads; // 工作线程队列
    std::mutex m_conditional_mutex; // 线程休眠锁互斥变量
    std::condition_variable m_conditional_lock; // 线程环境锁，可以让线程处于休眠或者唤醒状态
public:
    // 线程池构造函数
    ThreadPool(const int n_threads = 4)
        : m_threads(std::vector<std::thread>(n_threads)), m_shutdown(false)
    {
    }
    ThreadPool(const ThreadPool &) = delete;
    ThreadPool(ThreadPool &&) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;
    ThreadPool &operator=(ThreadPool &&) = delete;
    // Inits thread pool
    void init()
    {
        for (int i = 0; i < m_threads.size(); ++i)
        {
            m_threads.at(i) = std::thread(ThreadWorker(this, i)); // 分配工作线程
        }
    }
    // Waits until threads finish their current task and shutdowns the pool
    void shutdown()
    {
        m_shutdown = true;
        m_conditional_lock.notify_all(); // 通知，唤醒所有工作线程
        for (int i = 0; i < m_threads.size(); ++i)
        {
            if (m_threads.at(i).joinable()) // 判断线程是否在等待
            {
                m_threads.at(i).join(); // 将线程加入到等待队列
            }
        }
    }
    void wait() {
        while(!m_queue.empty())
        {
            //休眠0.05s
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            //cout<<"wait"<<endl;
        }
        //cout<<"wait end"<<endl;
        // 等待所有线程完成任务
        shutdown();
    }
    // Submit a function to be executed asynchronously by the pool
    template <typename F, typename... Args>
    auto submit(F &&f, Args &&...args) -> std::future<decltype(f(args...))>
    {
        // Create a function with bounded parameter ready to execute
        std::function<decltype(f(args...))()> func = std::bind(std::forward<F>(f), std::forward<Args>(args)...); // 连接函数和参数定义，特殊函数类型，避免左右值错误
        // Encapsulate it into a shared pointer in order to be able to copy construct
        auto task_ptr = std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);
        // Warp packaged task into void function
        std::function<void()> warpper_func = [task_ptr]()
        {
            (*task_ptr)();
        };
        // 队列通用安全封包函数，并压入安全队列
        m_queue.enqueue(warpper_func);
        // 唤醒一个等待中的线程
        m_conditional_lock.notify_one();
        // 返回先前注册的任务指针
        return task_ptr->get_future();
    }
};
class FileHandlePool {
public:

    int openFile(const string& filePath, int flags) {
        {
            std::shared_lock<std::shared_mutex> readLock(handlesMut);
            auto it = handles.find(filePath);
            if (it != handles.end()) {
                return it->second;
            }
        }
        int fd = ::open(filePath.c_str(), flags, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            perror("Error opening file");
            return -1;
        }
        {
            std::unique_lock<std::shared_mutex> writeLock(handlesMut);
            handles[filePath] = fd;
        }
        return fd;
    }
private:
    unordered_map<string,int> handles;
    shared_mutex handlesMut;
};
FileHandlePool pool;
//string logPath1 = "/mnt/bigdata/shot";//"../log" //"/mnt/NVMe/log"
//string logPath2 = "/mnt/md0/fff";
string logPath1 = "/mnt/target/bplustreetest_test/log_data.bin";

// 生成随机数据填充数组
void fillRandomData(char* data, int size) {
    // 定义随机字符集，这里使用可打印 ASCII 字符集
    const char printableChars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+[]{};:'\",.<>?/\\|`~";

    // 计算随机字符集的长度
    const int printableCharsLength = sizeof(printableChars) - 1;

    // 填充数组
    for (int i = 0; i < size; ++i) {
        // 从随机字符集中随机选择一个字符填充数组
        data[i] = printableChars[rand() % printableCharsLength];
    }
}
uint64_t MurmurHash64A ( const void * key, int len)
{
    uint64_t seed=0;
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);
    while(data != end)
    {
        uint64_t k = *data++;
        k *= m; 
        k ^= k >> r; 
        k *= m;      
        h ^= k;
        h *= m; 
    }
    const unsigned char * data2 = (const unsigned char*)data;
    switch(len & 7)
    {
    case 7: h ^= ((uint64_t) data2[6]) << 48;
    case 6: h ^= ((uint64_t) data2[5]) << 40;
    case 5: h ^= ((uint64_t) data2[4]) << 32;
    case 4: h ^= ((uint64_t) data2[3]) << 24;
    case 3: h ^= ((uint64_t) data2[2]) << 16;
    case 2: h ^= ((uint64_t) data2[1]) << 8;
    case 1: h ^= ((uint64_t) data2[0]);
            h *= m;
    };   
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
} 

string generateID(const string& filepath, uint64_t uid) {
    // 获取 filepath 中的父目录路径和文件名
    size_t pos = filepath.find_last_of('/');
    string parent_dir = filepath.substr(0, pos); // 父目录路径
    string filename = filepath.substr(pos + 1); // 文件名
    // 对父目录路径和文件名分别进行 MurmurHash2 算法散列
    uint64_t hash_parent_dir = MurmurHash64A(parent_dir.c_str(), parent_dir.length());
    uint64_t hash_filename = MurmurHash64A(filename.c_str(), filename.length());
    // 创建缓冲区来保存拼接后的字节流
    string result(24, '\0');
    // 将 uid 拷贝到缓冲区的开头，占用 8 个字节
    memcpy(&result[0], &uid, 8);
    char * ptr = &result[0]+8;
    memcpy(ptr, &hash_parent_dir, sizeof(hash_parent_dir));
    ptr += sizeof(hash_parent_dir);
    memcpy(ptr, &hash_filename, sizeof(hash_filename));
    return result;
}
int fd1;
int fd2;
// 读取磁盘操作函数
void readDisk(uint64_t found_address,char* buffer) {
    if (!buffer) {
        cerr << "Error allocating memory for buffer." << endl;
        return;
    }
    ssize_t bytesRead;
    // 使用pread()直接读取文件内容到缓冲区
    if(found_address/(leafcount*50000)==0)
    {
        bytesRead = pread(fd1, buffer, pagesize*1024, found_address*pagesize*1024);
    }else
    {
        bytesRead = pread(fd2, buffer, pagesize*1024, found_address%(leafcount*50000)*pagesize*1024);
    }
    if (bytesRead == -1) {
        cerr << "Error reading file: "  << " - " << strerror(errno) << endl;
        free(buffer);
        return;
    }
}
bool readfromLeafNode(MasstreeWrapper& masstree_wrapper, const string& key) {
    // 创建一个向量来存储扫描结果
    vector<pair<string,uint64_t>> result;
    masstree_wrapper.scan(key, 1, result);
    if (result.empty()) {
        cout << "Leaf node not found for key: " << key << endl;
        return false;
    } else {
        //比较key的前四个字节与result[0]的前8个字节是否一样
        if(key.substr(0,8)!=result[0].first.substr(0,8))
        {
            cout << "Uid not right, key: " << key << endl;
            return false;
        }
        uint64_t oldnum = result[0].second;
        char *buffer = (char*)aligned_alloc(4096, 1024*pagesize);
        // 从磁盘中读取叶子节点的数据到 buffer 中
        readDisk(oldnum, buffer);
        //cout<<"oldnum:"<<oldnum<<endl;
        //buffer的最后sizeofsize个字节存储size
        sizetype size;
        memcpy(&size, buffer + 1024*pagesize-sizeofsize, sizeofsize);
        //cout<<"size:"<<(int)size<<endl;
        // 假设数据格式为键值对依次存储
        int low=0;
        int high=size-1;
        int mid;
        string key2=key.substr(8);
        const char* keyCStr=key2.c_str();
        while (low <= high) {
                mid = low + (high - low) / 2;
                int compareResult = memcmp(buffer + mid * (keysize+valuesize), keyCStr, keysize);
                if (compareResult == 0) {
                    // 释放内存
                    free(buffer);
                    return true;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        // 释放内存
        free(buffer);
        //cin>>mid;
        cout << "Value not found for key: " << key << endl;
        return false;
    }
}
struct FileInfo {
    string key;
    char type;
    uint64_t size;       // 文件大小
    time_t createTime;   // 文件创建时间
    time_t lastModifiedTime;  // 文件最近一次修改时间
    time_t lastAccessTime;    // 文件最近一次访问时间
    // 定义 < 运算符用于比较
    bool operator<(const FileInfo& other) const {
        return key < other.key;
    }
};
FileInfo fileInfos[userfilenum];
void readvectorfromtxt(const string& filePath)
{
    ifstream inputFile(filePath);
    if (!inputFile) {
        cerr << "Error opening input file: " << filePath << endl;
        return;
    }
    FileInfo fileInfo;
    string encryptedPath;
    string key;
    for(int i=0;i<userfilenum;i++)
    {
        getline(inputFile, encryptedPath);
        key = generateID(encryptedPath,1);
        //fileInfo.key删掉key的8个字节
        fileInfo.key=key.substr(8);
        getline(inputFile, encryptedPath);
        inputFile >> fileInfo.type >> fileInfo.size >> fileInfo.createTime >> fileInfo.lastModifiedTime >> fileInfo.lastAccessTime;
        inputFile.ignore(); // 忽略换行符
        fileInfos[i]=fileInfo;
    }
    inputFile.close();
    sort(fileInfos, fileInfos+userfilenum, [](const FileInfo& a, const FileInfo& b) {
        return a.key < b.key;
    });
}
int fd;
void asyncWriteToFile(char* content, uint32_t uid1, uint32_t uid2) {
    for(int uid=uid1;uid<=uid2;uid++)
    {
        if (pwrite(fd, content, leafcount*pagesize*1024,(uint64_t)(uid-1)*leafcount*pagesize*1024) != leafcount*pagesize*1024) {
            std::cerr << "Error writing to file"  << std::endl;
        }
    }
}
std::vector<std::future<void>> asyncTasks;
// 新增函数：等待所有异步写入完成
void waitForAllWrites() {
    // 遍历所有异步任务的future并等待它们完成
    for(auto& task : asyncTasks) {
        task.get();
    }
    // 清理asyncTasks容器，释放资源
    asyncTasks.clear();
}
void insertallfile(MasstreeWrapper& mt_wrapper,uint64_t uid)
{
    uint32_t nownumEntries;
    uint32_t ssize;
    sizetype size;
    string key(8, '\0');
    memcpy(&key[0], &uid, 8);
    int k=leafnum-1;
    mt_wrapper.thread_init(uid);
    for(int i=0;i<leafcount-1;i++)
    {
        mt_wrapper.insert(key+fileInfos[k].key,(uid-1)*leafcount+i);
        k+=leafnum;
    }
    mt_wrapper.insert(key+fileInfos[userfilenum-1].key,(uid-1)*leafcount+leafcount-1); 
}
void insertfromvector(MasstreeWrapper& mt_wrapper,uint64_t firstuid,uint64_t lastuid,int numThreads,int tasknum)
{
    //计时
    struct timeval t1,t2;
    double timeuse;
    char storageLocation[5];
    char otherProperties[valuesize-38];
    fillRandomData(storageLocation, sizeof(storageLocation));
    fillRandomData(otherProperties, sizeof(otherProperties));
    string key;
    uint32_t nownumEntries;
    int address;
    sizetype size;
    int k=0;
    //写最后一个文件
    gettimeofday(&t1,NULL);
    char* oss;
    if (posix_memalign((void**)&oss, 4096, pagesize*1024*leafcount) != 0) {
        perror("posix_memalign for oss");
        return;
    }
    char* ptr=oss;
    for(int i=0;i<leafcount-1;i++)
    {
        for(int j=0;j<leafnum;j++)
        {
            key=fileInfos[k].key;
            memcpy(ptr,key.c_str(),keysize);
            ptr+=keysize;
            *ptr=fileInfos[k].type;
            ptr++;
            memcpy(ptr,&fileInfos[k].size,8);
            ptr+=8;
            memcpy(ptr,&fileInfos[k].createTime,8);
            ptr+=8;
            memcpy(ptr,&fileInfos[k].lastModifiedTime,8);
            ptr+=8;
            memcpy(ptr,&fileInfos[k].lastAccessTime,8);
            ptr+=8;
            memcpy(ptr,storageLocation,5);
            ptr+=5;
            memcpy(ptr,otherProperties,valuesize-38);
            ptr+=valuesize-38;
            k++;
        }
        ptr+=emptynum;
        size=leafnum;
        memcpy(ptr,&size,sizeofsize);
        ptr+=sizeofsize;
    }
    for(int j=0;j<(userfilenum-(leafcount-1)*leafnum);j++)
    {
        key=fileInfos[k].key;
        memcpy(ptr,key.c_str(),keysize);
        ptr+=keysize;
        *ptr=fileInfos[k].type;
        ptr++;
        memcpy(ptr,&fileInfos[k].size,8);
        ptr+=8;
        memcpy(ptr,&fileInfos[k].createTime,8);
        ptr+=8;
        memcpy(ptr,&fileInfos[k].lastModifiedTime,8);
        ptr+=8;
        memcpy(ptr,&fileInfos[k].lastAccessTime,8);
        ptr+=8;
        memcpy(ptr,storageLocation,5);
        ptr+=5;
        memcpy(ptr,otherProperties,valuesize-38);
        ptr+=valuesize-38;
        k++;
    }
    size=(userfilenum-(leafcount-1)*leafnum);
    ptr+=emptynum+(leafnum-size)*(keysize+valuesize);
    memcpy(ptr,&size,sizeofsize);
    ptr+=sizeofsize;
    gettimeofday(&t2,NULL);
    timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    cout << "total dram time:" << timeuse << "s" << endl;
    gettimeofday(&t1,NULL);
    ThreadPool apool(numThreads);
    apool.init();
    int usrsPerTime=(lastuid-firstuid+1)/tasknum;
    int leave=(lastuid-firstuid+1)%tasknum;
    uint32_t u1;
    uint32_t u=firstuid;
    fd = open(logPath1.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0644);
    if (fd == -1) {
        std::cerr << "Error opening file"  << std::endl;
        return;
    }
    for(int i=0;i<tasknum;i++)
    {
        if(i<leave)
        {
            u1=u+usrsPerTime;
        }
        else
        {
            u1=u+usrsPerTime-1;
        }
        cout<<u<<"~"<<u1<<endl;
        //auto writeFuture = asyncWriteToFile(oss,u,u1);
        //asyncTasks.push_back(std::move(writeFuture));
        apool.submit(asyncWriteToFile,oss,u,u1);
        u=u1+1;
    }
    //waitForAllWrites();
    apool.wait();
    free(oss);
    sync();
    close(fd);
    gettimeofday(&t2,NULL);
    timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    cout << "total write time:" << timeuse << "s" << endl;
    gettimeofday(&t1,NULL);
    gettimeofday(&t2,NULL);
    timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    cout << "total insert time:" << timeuse << "s" << endl;
}
void recover(MasstreeWrapper mt_wrapper,uint64_t firstuid,uint64_t lastuid)
{
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    ThreadPool pool(60);
    pool.init();
    for(int u=firstuid;u<=lastuid;u++)
    {
        pool.submit(insertallfile,ref(mt_wrapper),u);
    }
    pool.wait();
    gettimeofday(&t2,NULL);
    timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
    cout << "total recover time:" << timeuse << "s" << endl;
}
// 获取进程的物理内存占用情况
void printMemoryUsage() {
    ifstream file("/proc/self/status");
    string line;
    while (getline(file, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            cout << "Memory usage: " << line.substr(7) << endl;
            break;
        }
    }
    file.close();
}
int main() {
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    MasstreeWrapper mt_wrapper;
    int choose;
    vector<string> allFilePaths;
    string encryptedPath;
    int n;
    uint32_t low;
    uint32_t high;
    int k;
    pid_t pid;
    int tasksPerThread;
    int usrsPerTime;
    int numThreads;
    int numTasks;
    int remainingTasks;
    int startTaskIndex;
    int endTaskIndex;
    int tasksToProcess;
    ifstream inputFile;
    const string filePath =  "../txt/filename.txt";
    string infoFilePath = "../txt/file_info.txt";
    allFilePaths.clear();
                    // 打开保存文件信息的文本文件,生成随机路径
                inputFile.open(filePath);
                srand((unsigned)time(NULL));
                for (int j=0;j<100;j++) { //加随机数量 从0.1B往上加
                        //随机生成n
                        n = rand()%userfilenum;
                        //忽略文件前n行
                        for(int m=0;m<n;m++)
                        {
                            getline(inputFile,encryptedPath);
                        }
                        //读取第n行的文件路径
                        getline(inputFile,encryptedPath);
                        //存入vector
                        allFilePaths.push_back(encryptedPath);
                        //清空文件流
                        inputFile.clear();
                        //文件流指针回到文件头
                        inputFile.seekg(0, ios::beg);
                }
                if (!inputFile) {
                    cerr << "Error opening input file: " << filePath << endl;
                }
                inputFile.close();
    vector<thread> threads;
    readvectorfromtxt(infoFilePath);
    //recover(mt_wrapper,1,10000);
    cout << "1.获取当前进程的 PID" <<endl;
    cout << "2.打印内存占用信息" <<endl;
    cout << "3.生成随机路径" <<endl;
    cout << "4.导入数据库" <<endl;
    cout << "5.查询文件信息" <<endl;
    cout << "6.删除数据库" <<endl;
    cout << "7.多线程查数据库" <<endl;
    cout << "-1.退出" <<endl;
    cout << "请输入：" <<endl;
    cin >> choose;
    cout <<endl;
    while(choose!=-1)
    {
        switch(choose)
        {
            case 1:
                // 获取当前进程的 PID
                pid = getpid();
                // 打印当前进程的 PID
                cout << "当前进程的 PID 是：" << pid << endl;
                break;
            case 2:
                printMemoryUsage();
                break;
            case 3:
                allFilePaths.clear();
                // 打开保存文件信息的文本文件,生成随机路径
                inputFile.open(filePath);
                srand((unsigned)time(NULL));
                for (int j=0;j<100;j++) { //加随机数量 从0.1B往上加
                        //随机生成n
                        n = rand()%userfilenum;
                        //忽略文件前n行
                        for(int m=0;m<n;m++)
                        {
                            getline(inputFile,encryptedPath);
                        }
                        //读取第n行的文件路径
                        getline(inputFile,encryptedPath);
                        //存入vector
                        allFilePaths.push_back(encryptedPath);
                        //清空文件流
                        inputFile.clear();
                        //文件流指针回到文件头
                        inputFile.seekg(0, ios::beg);
                }
                if (!inputFile) {
                    cerr << "Error opening input file: " << filePath << endl;
                }
                inputFile.close();
                break;
            case 4:
                cout << "请输入待导入用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入要开启的线程数：" << endl;
                cin >> numThreads;
                cout << "请输入任务个数：" << endl;
                cin >> numTasks;
                gettimeofday(&t1,NULL);
                {
                    insertfromvector(mt_wrapper,low,high,numThreads,numTasks);
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(1000000*(high-low+1)) << "s" << endl;
                break;
            case 5:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                sync();
                //遍历查询filename中的所有文件路径
                gettimeofday(&t1,NULL);
                fd1=open(logPath1.c_str(),O_RDONLY | O_DIRECT);
                //fd2=open(logPath2.c_str(),O_RDONLY | O_DIRECT);
                for(uint32_t i=low;i<=high;i++)
                {
                    for(int j=0;j<k;j++)
                    {
                        readfromLeafNode(mt_wrapper, generateID(allFilePaths[j], i));
                    }
                }
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
                close(fd1);
                //close(fd2);
                break;
            case 6:
                // 删除 log 目录下所有文件 
                break;
            case 7:
                cout << "请输入待查询用户ID范围low和high:" <<endl;
                cin >> low >>high;
                cout << "请输入每个用户的查询路径个数：" <<endl;
                cin >> k;
                cout << "请输入要开启的线程数：" << endl;
                cin >> numThreads;
                sync();
                // 获取开始时间
                gettimeofday(&t1, NULL);
                fd1=open(logPath1.c_str(),O_RDONLY | O_DIRECT);
                //fd2=open(logPath2.c_str(),O_RDONLY | O_DIRECT);
                threads.clear();
                numTasks=(high - low + 1) * k;
                // 计算每个线程需要处理的任务数量
                tasksPerThread = numTasks / numThreads;
                // 循环创建线程执行查询操作
                for (int t = 0; t < numThreads; ++t) {
                    threads.emplace_back([low,k,numTasks, tasksPerThread, numThreads, t, &mt_wrapper,&allFilePaths]() { // 在捕获列表中添加 mt_wrapper
                    // 计算该线程负责的任务范围
                    int startTaskIndex = t * tasksPerThread;
                    int endTaskIndex = (t == numThreads - 1) ? numTasks - 1 : startTaskIndex + tasksPerThread - 1;
                    for (int taskIndex = startTaskIndex; taskIndex <= endTaskIndex; ++taskIndex) {
                        // 计算当前任务所属的用户ID和文件路径索引
                        int userID = low + taskIndex / k;
                        int filePathIndex = taskIndex % k;
                        readfromLeafNode(mt_wrapper, generateID(allFilePaths[filePathIndex], userID));
                    }             
                    });
                }
                // 等待所有线程执行完毕
                for (auto& thread : threads) {
                    thread.join();
                }
                close(fd1);
                //close(fd2);
                gettimeofday(&t2,NULL);
                timeuse = t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec)/1000000.0;
                cout << "total time:" << timeuse << "s" << endl;
                cout << "average find time:" << timeuse/(k*(high-low+1)) << "s" << endl;
                break;
            default:
                cout << "输入不合法，请重新输入" <<endl;
        }
        cout <<endl;
        cout << "1.获取当前进程的 PID" <<endl;
        cout << "2.打印内存占用信息" <<endl;
        cout << "3.生成随机路径" <<endl;
        cout << "4.导入数据库" <<endl;
        cout << "5.查询文件信息" <<endl;
        cout << "6.删除数据库" <<endl;
        cout << "7.多线程查数据库" <<endl;
        cout << "-1.退出" <<endl;
        cout << "请输入：" <<endl;
        cin >> choose;
    } 
    return 0;
}    