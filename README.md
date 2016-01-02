Mycat IPC
---
Mycat java进程间超高性能通信服务框架
####特性
   1. 使用共享内存映射的技术，多进程之间数据传递直接操作内存，性能极高
   2. 实现了无锁多进程/多线程读写的逻辑，多个进程可以同时并发读写
   3. HP ZBook笔记本测试，单一队列，两个进程通信，最高高达每秒500万的消息传输，3组进程分别对应3个队列，总和超过1000万每秒消息



####使用方式
	1. 启动 io.mycat.ipc.Reader 创建一个空白队列，等待和读取消息
	2. 启动 io.mycat.ipc.Writer 写入消息

HP ZBook 17上 在Eclipse里启动上述两个进程的测试结果截图
![image](https://raw.githubusercontent.com/MyCATApache/Mycat-IPC/master/images/perf1.png)

![image](https://raw.githubusercontent.com/MyCATApache/Mycat-IPC/master/images/cpu.png)
注意，由于Windows任务调度的问题，经常会有Read进程得不到调度而停顿的现象，从屏幕输出可以看到此现象。
####限制
	1. JDK 要求 SunJDK 1.8


####内存格式图
![image](https://github.com/huangll99/Mycat-IPC/blob/master/images/memory-format.jpg)
	