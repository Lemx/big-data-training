Execute run.sh.

List of parameters:  
1. address to connect to, default: localhost  
2. port to listen on, default: 55552  
3. path to JAR, default: target/lab5-1.0.jar  
4. cores to allocate for AppMaster, default: 1  
5. memory to allocate for AppMaster, default: 256  
6. memory to allocate for every AppContainer, default: 2048  
7. cores to allocate for every AppContainer, default: 2  
8. containers to spawn, default: 1  
9. priority to run container with, default: 0  
10. iterations to make, default: 10000  
11. precision, default: 100000  

####OR

Build project with Maven, execute "hadoop jar target/lab5-1.0.jar 1 256 target/lab5-1.0.jar 55552" and open app_master_ip:55552 in your browser

