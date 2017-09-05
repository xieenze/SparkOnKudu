
Spark On Kudu 案例吐血总结
-------------

结合我在Evcard和eBay实习中对于kudu的配置和使用，对kudu的分布式环境配置和应用程序开发作一些微小的贡献。毕竟现在网上很难找到kudu有关的实用资料。我这份总结应该是全网目前最详细的kudu ***离线配置*** + ***开发*** 资料，当然我也找了很多大神交流过，现在回馈给广大码农朋友。不要问我是谁，我叫雷锋。



> **Note:**
>  1. kudu的配置
>  2. spark结合kudu开发应用程序
>  pre：本案例使用的服务器是 ***centos7*** ，一共 ***两台*** 机器，一台机器作为master和tserver，另一台机器作为tserver。


----------

Apache Kudu官网：http://kudu.apache.org/ 建议参考官网的配置，结合我的一起看，我也是从官网总结的，不过官网有些地方写的很不清楚

#### <i class="icon-file"></i> 1. kudu的配置

 - 先决条件
	 - 一台或多台master机器（奇数）
	 - 一台或者多台tserver机器（123。。。）
	 - 设置时间同步`ntp`




 - 在centos上安装kudu
	1. 下载kudu的所有[rpm安装包](http://archive.cloudera.com/kudu/redhat/7/x86_64/kudu/5/RPMS/x86_64/)，一共6个  
	2. 手动安装 `sudo rpm -ivh xxx.rpm`
	3. 注意：
		 - master机器上不需要安装tserver的rpm
		 - tserver机器上不需要安装master的rpm
		 - 一台机可以既是master又是tserver，也就是两个都装
		 - 如果出现安装失败的情况提前安装cyrus-sasl-plain，lsb插件
 
	4. 安装包要按照如下顺序安装
  
       - `sudo rpm -ivh kudu-debuginfo-1.4.0+cdh5.12.0+0-1.cdh5.12.0.p0.24.el7.x86_64.rpm`
       - `sudo rpm -ivh kudu-client0-1.4.0+cdh5.12.0+0-1.cdh5.12.0.p0.24.el7.x86_64.rpm`
       - `sudo rpm -ivh kudu-client-devel-1.4.0+cdh5.12.0+0-1.cdh5.12.0.p0.24.el7.x86_64.rpm`
       - `sudo rpm -ivh kudu-1.4.0+cdh5.12.0+0-1.cdh5.12.0.p0.24.el7.x86_64.rpm` 
       - `sudo rpm -ivh kudu-tserver/master-1.4.0+cdh5.12.0+0-1.cdh5.12.0.p0.24.el7.x86_64.rpm`
    5. 修改master和tserver上的配置文件保证集群配置成功
	      - 在master机器上执行 `vim /etc/default/kudu-master`  
	    将  
	    `export FLAGS_rpc_bind_addresses=0.0.0.0:7051`  
	    修改为   
	    `export FLAGS_rpc_bind_addresses=master的ip地址:7051`  
	      - 分别在tserver机器上执行  
		 `vim /etc/kudu/conf/tserver.gflagfile`  
		 在末尾增加  
		 `--tserver_master_addrs=master的ip地址:7051`  
	      - 启动master和tserver  
	  `sudo service kudu-master start`  
	  `sudo service kudu-tserver start`  
	      - 停止master和tserver  
	  `sudo service kudu-master stop`  
	  `sudo service kudu-tserver stop`
    6. 访问web界面监控kudu集群情况  
	  master监控：http://masterURL:8051(主要看这个)  
	  tserver监控：http://tserverURL:8050   
	  Kudu主进程在8051端口上为其Web界面提供服务。该界面暴露了几个页面，其中包含有关群集状态的信息：  
	     - slave节点服务器列表，其主机名和上次心跳时间。  
	     - table列表，包括每个表的表结构和分片位置信息。  
	     - 在kudu master服务器上执行命令：`kudu table list master` 可以查看有哪些表  


----------

#### <i class="icon-file"></i> 2. spark + kudu 应用程序开发
1. 实现增删改查，通过KuduContext的API
2. 实现修改表结构，如增加，删除，重命名列，通过KuduClient的API  
暂时先写到这里，现成的代码看这个例子  
https://github.com/xieenze/SparkOnKudu/blob/master/src/main/scala/com/spark/test/KuduCRUD.scala  
