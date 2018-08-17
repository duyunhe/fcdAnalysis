# fcdAnalysis
>根据浮动车辆计算路况原型测试
算法基于地图匹配，将获取到的GPS点通过基础的点到直线匹配算法，并配合车辆行驶信息进行筛选判断，将GPS点映射到地图上。之后根据该车在两次GPS采样时间内的完整路径，估算出其在经过路径上的速度，并对杭州出租车加以融合，得到道路在一段时间内的平均车速。

1.  map_matching.py
>地图匹配模块
2.  fetch_taxi_data.py
> 获取出租车信息模块
3.  fetch_FCD_data.py
> 获取浮动车信息模块
4.  fcd_processor.py
> 路况分析控制模块
5.  estimate_speed.py
> 道路速度估算模块
6.  geo.py
> 地图运算模块
7.  map_struct.py
> 基本数据结构
8.  DBConn
> 数据库连接